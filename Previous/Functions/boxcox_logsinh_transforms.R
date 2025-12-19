# Boxcox transforms ------------------------------------------------------------

boxcox_transform <- function(y, lambda = 0, lambda_2) {

  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "boxcox_transform",
        "parameters" = c("lambda")
      )
    )
  }
  
  transformed_result <- ((y + lambda_2)^lambda - 1) / lambda
  
  # to make it work with log
  # get true/false vector of lambda values if near zero
  # if true replace with log(...)
  near_zero_lambda <- lambda <= .Machine$double.eps^0.5 #dplyr::near(lambda, y = 0, tol = .Machine$double.eps^0.5)
  
  transformed_result[near_zero_lambda] <- log(y[near_zero_lambda] + lambda_2)
  
  return(transformed_result)
 

}



inverse_boxcox_transform <- function(yt, lambda = 0, lambda_2) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "inverse_boxcox_transform",
        "parameters" = c("lambda")
      )
    )
  }
  
  realspace_result <- ((yt * lambda) + 1)^(1 / lambda) - lambda_2
  
  # to make it work with log
  # get true/false vector of lambda values if near zero
  # if true replace with log(...)
  near_zero_lambda <- lambda <= .Machine$double.eps^0.5#dplyr::near(lambda, y = 0, tol = .Machine$double.eps^0.5)
  
  realspace_result[near_zero_lambda] <- exp(yt[near_zero_lambda]) - lambda_2
  
  return(realspace_result)
}





# Log-sinh transform -----------------------------------------------------------
log_sinh_transform <- function(b, y, offset) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "log_sinh_transform",
        "parameters" = c("b")
      )
    )
  }
  
  
  #(1 / b) * log(sinh(-a + (b * (y + offset))))
  (1 / b) * log(sinh(b * (y + offset)))
}


asinh_exp_approximation <- function(x) {
  # Input: asinh(exp(x))
  
  #Testing tim's log-sinh - add to inverse_log_sinh then delete
  #x <- 0.1:1000
  #y <- asinh(exp(x))
  #y2 <- log(exp(x) + sqrt(exp(2 * x) + 1))
  #y3 <- x + log(1 + sqrt(exp(2 * x) + 1) / exp(x))
  #y4 <- x + log(1 + sqrt(1 + (1 / exp(2 * x))))
  #y5 <- (is.infinite(y4) * y4) + (is.infinite(y4) * (x + log(2))) - I am not sure what this is for
  
  x + log(1 + sqrt(1 + (1 / exp(2 * x))))
  
}



inverse_log_sinh_transform <- function(b, z, offset) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "inverse_log_sinh_transform",
        "parameters" = c("b")
      )
    )
  }
  
  
  
  # Changing bounds may address this issue...
  # If any streamflow value is infinite the approximation is done for everything
  if (any(is.infinite(exp(b * z)))) {
    asinh_component <- asinh_exp_approximation(b * z)
  } else {
    asinh_component <- asinh(exp(b * z))
  }
  
  (asinh_component / b) - offset
  
}


# For objective_function_setup -------------------------------------------------
# if you want to add more transform methods add them here

select_streamflow_transform_method <- function(timeseries, parameter_set, streamflow_transform_method, offset) {
  
  # call the streamflow_transfer_method to get name of method perform
  # different operation depending on name
  streamflow_transform_method_name <- streamflow_transform_method()[[1]]
  
  # Transform observed_streamflow into log-sinh space
  if (streamflow_transform_method_name == "log_sinh_transform") {
  
    log_sinh_b <- parameter_set[nrow(parameter_set) - 1, ]
    
    matrix_log_sinh_b <- matrix(
      log_sinh_b,
      nrow = nrow(timeseries),
      ncol = ncol(timeseries),
      byrow = TRUE
    )
    
    transformed_observed_streamflow <- log_sinh_transform(
      b = matrix_log_sinh_b,
      y = timeseries,
      offset = offset
    )
    
    return(transformed_observed_streamflow)
    
    # Transform observed_streamflow into boxcox space
  } else if (streamflow_transform_method_name == "boxcox_transform") {
    
    boxcox_lambda <- parameter_set[nrow(parameter_set) - 1, ]
    
    matrix_boxcox_lambda <- matrix(
      boxcox_lambda,
      nrow = nrow(timeseries),
      ncol = ncol(timeseries),
      byrow = TRUE
    )
    
    transformed_observed_streamflow <- boxcox_transform(
      y = timeseries,
      lambda = matrix_boxcox_lambda,
      lambda_2 = offset
    )
    
    return(transformed_observed_streamflow)
    
  } else if (streamflow_transform_method_name == "inverse_log_sinh_transform") {
    
    log_sinh_b <- parameter_set[nrow(parameter_set) - 1, ]
    
    matrix_log_sinh_b <- matrix(
      log_sinh_b,
      nrow = nrow(timeseries),
      ncol = ncol(timeseries),
      byrow = TRUE
    )
    
    realspace_observed_streamflow <- inverse_log_sinh_transform(
      b = matrix_log_sinh_b,
      z = timeseries,
      offset = offset
    )
    
    return(realspace_observed_streamflow)
    
    
  } else if (streamflow_transform_method_name == "inverse_boxcox_transform") {
    
    boxcox_lambda <- parameter_set[nrow(parameter_set) - 1, ]
    
    matrix_boxcox_lambda <- matrix(
      boxcox_lambda,
      nrow = nrow(timeseries),
      ncol = ncol(timeseries),
      byrow = TRUE
    )
    
    realspace_observed_streamflow <- inverse_boxcox_transform(
      y = timeseries,
      lambda = matrix_boxcox_lambda,
      lambda_2 = offset
    )
    
    return(realspace_observed_streamflow)
  }
  
}

get_streamflow_transform_method <- function() {
  c(log_sinh_transform, boxcox_transform)
}
