import math

def predict_footprint_from_size( x, y, z ):
  return x*y*z

def predict_footprint_from_size_time( x, y, z, timesteps ):
  return x * y * z * timesteps

def predict_footprint_from_size_time_processes( x, y, z, timesteps, p, q, r ):
  # assume timesteps add .1 % additional memory per timestep for the domain
  # assume an additional 20% memory penalty from multiple processes
  return (x * y * z +  ( x * y * z * (.001*timesteps) ) )* 1.2

def predict_footprint_from_size_processes( x, y, z, p, q, r ):
  # assume 1 time-step
  return predict_footprint_from_size_time_processes( x, y, z, 1, p, q, r)


def get_prediction_function():
  function = lambda x, y, z, timesteps, p, q, r: predict_footprint_from_size_time_processes( x, y, z, timesteps, p, q, r  )
  function_name = "predict_footprint_from_size_time_processes"
  return (function, function_name)
