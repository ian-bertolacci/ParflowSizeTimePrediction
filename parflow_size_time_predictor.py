#!/usr/bin/env python3

import re, os, sys, argparse, json, shutil, importlib.util, inspect, pprint
import subprocess as sp
import fileinput as fi

GLOBAL_DEBUG = False

pfrun_ouput_guards = "HOPEFULLY UNIQUE PREAMBLE THAT BLOCKS OFF VALID JSON CODE FROM OUTPUT"

pfrun_redefinition_code = r"""
proc Parflow::pfrun { runname args } {
  puts [format "pfrun %s has been intercepted by the size determination script." [format $runname $args]]

  set time_units  [pfget TimingInfo.BaseUnit]
  set start_time [pfget TimingInfo.StartTime]
  set stop_time [pfget TimingInfo.StopTime]
  set time_delta [expr $stop_time - $start_time]
  set time_steps [expr $time_delta / $time_units]

  set NX [pfget ComputationalGrid.NX]
  set NY [pfget ComputationalGrid.NY]
  set NZ [pfget ComputationalGrid.NZ]
  set NP [pfget Process.Topology.P]
  set NQ [pfget Process.Topology.Q]
  set NR [pfget Process.Topology.R]

  puts [format \
"
""" \
+ pfrun_ouput_guards + \
r"""
{
  \"grid\" :
  {
    \"NX\" : %s,
    \"NY\" : %s,
    \"NZ\" : %s
  },
  \"time\" : {
    \"time_steps\" : %s
  },
  \"process_topology\" : {
    \"NP\" : %s,
    \"NQ\" : %s,
    \"NR\" : %s
  }
}
""" \
+ pfrun_ouput_guards + \
r"""
" $NX $NY $NZ $time_steps $NP $NQ $NR]
}
"""

namespace_rx = re.compile( r"namespace\s+import\s+Parflow::\*" )
package_rx = re.compile( r"package\s+require\s+parflow" )
output_rx = re.compile( pfrun_ouput_guards + r"(?P<JSON_data>.+)" + pfrun_ouput_guards, flags=re.DOTALL )

def print_error(*args, **kwargs):
  print(*args, file=sys.stderr, **kwargs)

class ClobberError(RuntimeError):
  def __init__(this, path):
    this.path = path

  def __str__(this):
    return f"Path {path} already exists."

class FailedScriptExecutionError(RuntimeError):
  def __init__(this, command_list, stdout, stderr, exit_code ):
    this.command_list = command_list
    this.stdout = stdout
    this.stderr = stderr
    this.exit_code = exit_code
  def __str__(this):
    return "Script command \"" + (" ".join( this.command_list ) ) + f" failed with exit code {this.exit_code}.\np\nStandard Output:\n{stdout}\n\nStandard Error:\n{stderr}"

class InvalidScriptOutputError(RuntimeError):
  def __init__(this, output):
    this.output = output

  def __str__(this):
    return "Invalid output from script:\n" + "".join( ( '\t'+line for line in this.output ) ) + "\n"

class UnimplementedPredictionError(RuntimeError):
  def __init__(this, output):
    pass
  def __str__(this):
    return "Failed to find any valid prediction functions in analysis_module."

class InvalidPredictionValueError(RuntimeError):
  def __init__(this, value, function_name):
    this.value = value
    this.function_name = function_name

  def __str__(this):
    return f"Prediction function ({this.function_name}) produced an invalid value\n\tstr:"+str(this.value)+"\n\trepr: "+str(repr(this.value)) + "\n"

def move_file( src_path, dest_path, clobber=False ):
  if not clobber and os.path.exists( dest_path ):
    raise ClobberError( dest_path )

  shutil.move( src_path, dest_path )


def write_file( output_file_path, output_text, clobber=False ):
  if not clobber and os.path.exists(output_file_path):
    raise ClobberError( output_file_path )

  with open( output_file_path, "w" ) as output_file:
    output_file.write( output_text )

def parse_tcl_file( file_name ):
  lines = []
  require_line = None
  namespace_line = None
  pfrun_invocations = []

  for (index,line) in enumerate( fi.input( file_name ) ):
    lines.append( line )

    if package_rx.search( line ) != None:
      if require_line != None:
        print_error( f"Warning! Re-requiring package on line {index} (previously {require_line})." )
      require_line = index

    if namespace_rx.search( line ) != None:
      if namespace_line != None:
        print_error( f"Warning! Re-importing namespace on line {index} (previously {namespace_line})." )
      namespace_line = index

    if r"pfrun" in line:
      pfrun_invocations.append( index )

  if namespace_line == None:
    print_error( "Warning! Expected Parflow namespace import was never found." )

  return {
    "contents": lines,
    "require_line"  : require_line,
    "namespace_line"  : namespace_line,
    "pfrun_invocations" : pfrun_invocations
    }


def convert_tcl_script( contents, require_line, namespace_line, pfrun_invocations ):
  new_contents = []

  for (index, line) in enumerate(contents):
    new_contents.append( line )
    if index == namespace_line:
      new_contents.append( pfrun_redefinition_code )

  return new_contents


def parse_and_convert_file( input_file_path ):
  data = parse_tcl_file( input_file_path )

  new_content = convert_tcl_script( data["contents"], data["require_line"], data["namespace_line"], data["pfrun_invocations"] )
  # Fileinput preserves line-endings
  new_text = "".join(new_content)
  if GLOBAL_DEBUG:
    print_error("="*50 + f"\nNew Script\n" + "-"*50 + f"\n{new_text}\n" + "="*50)
  return new_text


def process_file( input_file_path, output_file_path, clobber=False ):
  text = parse_and_convert_file( input_file_path )
  write_file( output_file_path, text, clobber )


def run_script( script_path, arguments, tcl_shell="tclsh", exact_command=False ):

  if exact_command:
    command = arguments
  else:
    command = [tcl_shell, script_path, *arguments]
  process = sp.Popen( command , stdout=sp.PIPE, stderr=sp.PIPE)

  stdout, stderr = process.communicate()

  exit_status = process.wait()

  stdout = stdout.decode("utf-8")
  stderr = stderr.decode("utf-8")
  if GLOBAL_DEBUG:
    print_error( f"> {' '.join(command)}\nStandard Output:\n{stdout}\n\nStandard Error:\n{stderr}" )

  if exit_status != 0:
    raise FailedScriptExecutionError( command, stdout, stderr, exit_status )

  return stdout

def parse_script_output( output_text ):
  m = output_rx.search( output_text)
  if m == None:
    raise InvalidScriptOutputError( output_text )
  json_data = json.loads( m.group("JSON_data") )
  return json_data


def process_script( script_path, arguments, tcl_shell="tclsh", exact_command=False ):
  output = run_script( script_path, arguments, tcl_shell, exact_command )
  json_data = parse_script_output( output )
  return json_data


def is_legal_prediction_value( value ):
  return value != None \
    and type(value) in [int, float] \
    and float(value) >= 0.0

def predict_footprint( data, prediction_function, prediction_function_name ):

  nx=data["grid"]["NX"]
  ny=data["grid"]["NY"]
  nz=data["grid"]["NZ"]
  timesteps=data["time"]["time_steps"]
  np=data["process_topology"]["NP"]
  nq=data["process_topology"]["NQ"]
  nr=data["process_topology"]["NR"]

  prediction_value = prediction_function( nx, ny, nz, timesteps, np, nq, nr)

  if not is_legal_prediction_value(prediction_value):
    raise InvalidPredictionValueError(prediction_value, prediction_function_name)

  return prediction_value

def write_json( output_file_path, data, clobber=False ):
  if not clobber and os.path.exists(output_file_path):
    raise ClobberError( output_file_path )

  with open( output_file_path, "w" ) as output_file:
    json.dump( data, output_file )


exit_codes = {
  "success" : 0,
  "internal_error" : -1,
  "command_line_error" : -2,
  "clobber_error" : -3,
  "prediction_error" : -4,
  "prediction_module_error" : -5
}

def main( argv=sys.argv ):
  script_root_path = os.path.abspath( os.path.dirname( argv[0] ) )
  global GLOBAL_DEBUG

  default_output_suffix=".size_determiniation.output.tcl"
  default_backup_suffix=".size_determiniation.automated_backup.original.tcl"
  default_tcl_shell = "tclsh"
  default_prediction_module = script_root_path+"/default_prediction_module.py"

  exit_code_string = "Exit codes:\n" + "".join( ( f"\t{name}: {code}\n" for (name, code) in exit_codes.items() ) )

  parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,epilog=exit_code_string)
  parser.add_argument("file_path", type=str, help="Path to input tcl script.")

  parser.add_argument("execution_arguments", type=str, nargs="*", default="", help="Arguments supplied to execute tcl script, if any.")
  parser.add_argument("--exact_command", action="store_true", default=False, help="When set, execution_arguments will be executed exactly as specified, meaning no wrapping with tcl command will be used." )

  parser.add_argument("--output", type=str, default=None, help=f"Name of output file. (Default: <file_path>{default_output_suffix})")
  parser.add_argument("--backup_suffix", type=str, default=default_backup_suffix, help=f"Suffix used when --move_existing set. (Default <file_path>{default_backup_suffix})")
  parser.add_argument("--replace_existing", action="store_true", default=False, help="When set, moves renames input file with backup suffix, writes output file to input file's original path." )
  parser.add_argument("--enable_clobber", action="store_true", default=False, help="When set, allows overwriting of existing files." )

  parser.add_argument("--tcl_shell", type=str, default=default_tcl_shell, help=f"Command for executing tcl scripts. (Default {default_tcl_shell})")
  parser.add_argument("--prediction_module", type=str, default=default_prediction_module, help=f"Path to prediction module. (Default {default_prediction_module})")

  parser.add_argument("--json_output", type=str, default=None, help="Path to write report output to instead of standard out.")

  # NOTE: This functionality is available, but need clean way of implementing this from the module side.
  #parser.add_argument("--prediction_function", type=str, default=None, help=f"Path to prediction module. (Default is function returned by module's 'get_prediction_function' function )")

  parser.add_argument("--debug", default=False, action="store_true")

  args = parser.parse_args( argv[1:] )

  GLOBAL_DEBUG=args.debug

  # First, check that CLI arguments are at least correct
  setup_is_correct = True

  if args.replace_existing and args.output:
    print_error( "Error: Cannot use --output and --replace_existing flags.")
    setup_is_correct = False

  if args.file_path == args.output:
    print_error( "Error: input and output cannot be same file.")
    setup_is_correct = False


  # Exit if CLI arguments are incorrect
  if not setup_is_correct:
    print_error("There were errors from command line flag usage.")
    print_error("Please check usage.")
    parser.print_help(file=sys.stderr)
    return exit_codes["command_line_error"]

  # Setup application
  # Load prediction module
  args.prediction_module = os.path.abspath(args.prediction_module)
  spec = importlib.util.spec_from_file_location("", args.prediction_module)
  prediction_module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(prediction_module)

  module_dict = dict( inspect.getmembers(prediction_module) )

  if "get_prediction_function" in module_dict:
    prediction_function, prediction_function_name = prediction_module.get_prediction_function()
  else:
    print_error( f"Error: Prediction module ({args.prediction_module}) lacks the required get_prediction_function() function.\nAlternatively, choose prediction function with --prediction_function <function name>.")
    return exit_codes["prediction_module_error"]

  # NOTE: This functionality is available, but need clean way of implementing this from the module side.
  # Either point to modules get_prediction_function, or use specified function
  # if args.prediction_function != None:
  #   if args.prediction_function in module_dict:
  #     prediction_function = module_dict[args.prediction_function]
  #     prediction_function_name = args.prediction_function
  #   else:
  #     print( f"Error: Prediction module ({args.prediction_module}) lacks the specified prediction function {args.prediction_function}.\n")
  #     return exit_codes["prediction_module_error"]
  # elif "get_prediction_function" in module_dict:
  #   prediction_function, prediction_function_name = prediction_module.get_prediction_function()
  # else:
    #   print( f"Error: Prediction module ({args.prediction_module}) lacks the required get_prediction_function() function.\nAlternatively, choose prediction function with --prediction_function <function name>.\n")
  #   return exit_codes["prediction_module_error"]

  # if the output is not explicitly defined use
  if args.output == None:
    args.output = args.file_path + default_output_suffix

  # If replace_existing enabled, move files and setup new input/output paths
  if args.replace_existing:
    backup_path = args.file_path + args.backup_suffix
    try:
      move_file( args.file_path, backup_path, args.enable_clobber )
    except ClobberError as expt:
      if expt.path == backup_path:
        print_error( f"Error: destination for backup ({backup_path}) already exists.\nEither move existing file, or enabled clobber option." )
      else:
        print_error( f"""UNEXPECTED Error: During backup the following path exists causing a ClobberError:\n\t{expt.path}\nOffending path is *NOT* backup_path ({backup_path}).\nThis error is unexpected.\nPlease contact developers.\n(HIGHLY RISKY SUGGESTION) Enable clobber option if you know what you're doing.""" )
      parser.print_help(file=sys.stderr)
      return exit_codes["clobber_error"]

    args.output = args.file_path
    args.file_path = args.file_path + args.backup_suffix

  # Process file, including writing
  try:
    process_file( args.file_path, args.output, args.enable_clobber )
  except ClobberError as expt:
    if expt.path == args.output:
      print_error( f"Error: destination for output ({args.output}) already exists.\nEither move exitsing file, or enabled clobber option." )
    else:
      print_error( f"""UNEXPECTED Error: During processing the following path exists causing a ClobberError:\n\t{expt.path}\nOffending path is *NOT* args.output ({args.output}).\nThis error is unexpected.\nPlease contact developers.\n(HIGHLY RISKY SUGGESTION) Enable clobber option if you know what you're doing.""" )
    parser.print_help(file=sys.stderr)
    return exit_codes["clobber_error"]

  # Execute file
  try:
    script_output = process_script( args.output, args.execution_arguments, args.tcl_shell, args.exact_command)
  except Exception as e:
    print_error( f"Error: Exception caught during scrip processing:\n{e}." )
    return exit_codes["internal_error"]

  # Estimate memory footprint
  try:
    predicted_footprint = predict_footprint( script_output, prediction_function, prediction_function_name )
  except UnimplementedPredictionError as expt:
    print_error("Error: Prediction module is insuffeciently implemented.")
    return exit_codes["prediction_module_error"]
  except InvalidPredictionValueError as expt:
    print_error("Error:", expt)
    return exit_codes["prediction_error"]

  # create JSONified report and either print to stdout or write to file
  report = { **script_output, "footprint" : { "amount" : predicted_footprint, "units" : "kilobyte" } }

  if args.json_output != None:
    try:
     write_json( args.json_output, report, args.enable_clobber )
    except ClobberError as expt:
      if expt.path == args.json_output:
        print_error( f"Error: destination for json output ({args.json_output}) already exists.\nEither move existing file, or enabled clobber option." )
      else:
        print_error( f"""UNEXPECTED Error: During backup the following path exists causing a ClobberError:\n\t{expt.path}\nOffending path is *NOT* args.json_output ({args.json_output}).\nThis error is unexpected.\nPlease contact developers.\n(HIGHLY RISKY SUGGESTION) Enable clobber option if you know what you're doing.""" )
  else:
    pprint.pprint( report, width=-1 )

  return exit_codes["success"]

if __name__ == "__main__":
  main()
