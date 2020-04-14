
# This method is mandatory
# It must return a function accepting 7 arguments (nx, ny, nz, timesteps, np, nq, nr)
# as well as the name of the function.
# The returned function is the function used when predicting the size (in kilobytes)
# of the parflow application given the (nx, ny, nz, timesteps, np, nq, nr) parameters.
# This is the only requirement of a prediction module
def get_prediction_function():
  function = lambda x, y, z, timesteps, p, q, r: predict_footprint_from_size( x, y, z )
  function_name = "predict_footprint_from_size"
  return (function, function_name)

import math

def predict_footprint_from_size( x, y, z ):
  return -303000.3309 + x*806.6720 + y*815.8475 + x*y*4.8494  +  (x**2)*-0.0178 + (y**2)*-0.0154
