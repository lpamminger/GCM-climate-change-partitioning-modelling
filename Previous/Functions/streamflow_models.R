# Allow catchment data to be immediately used with streamflow models
catchment_data_directly_to_streamflow_model <- function(catchment_data, parameter_set, streamflow_model) {

    # If true extract either stop_start or full dataset - force stop_start
    stop_start_catchment_data <- catchment_data$stop_start_data_set
    
    # Make streamflow_model a model (no a char)
    streamflow_model <- noquote(streamflow_model)
    
    # Repeat parameter_set and stop_start_data
    parameter_set <- list(parameter_set)
    repeat_unique_combinations <- expand_grid(
      parameter_set, stop_start_catchment_data
      ) 
    
    # Make the variables the same names as the streamflow functions
    catchment_data <- repeat_unique_combinations |> 
      pull(stop_start_catchment_data) |> 
      unname()
    
    parameter_set <- repeat_unique_combinations |> 
      pull(parameter_set) |> 
      unname()
    
    # use recursion to force stop-start data
    streamflow_results <- pmap( # This works. map2 does not. Something to
      # do with variable names https://stackoverflow.com/questions/45541160/purrrpmap-with-user-defined-functions-and-named-list
      .l = list(catchment_data, parameter_set),
      .f = streamflow_model
    ) |> 
      list_c()
    
    
    # Create summary tibble
    result_tibble <- stop_start_catchment_data |> 
      list_rbind() |> 
      cbind(streamflow_results)
    
    return(result_tibble) 
}








# Non-drought models -----------------------------------------------------------
streamflow_model_precip_only <- function(catchment_data, parameter_set) { 
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_precip_only",
        "parameters" = c("a0", "a1")
        )
      )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_precip_only
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Model
  repeat_a0 + (repeat_a1 * repeat_precipitation)
  
}



streamflow_model_precip_seasonal_ratio <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_precip_seasonal_ratio",
        "parameters" = c("a0", "a1", "a4")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_precip_seasonal_ratio
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a4 <- parameter_set[3, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Model
  repeat_a0 + (repeat_a1 * repeat_precipitation) + (repeat_a4 * repeat_seasonal_ratio)
  
}



streamflow_model_precip_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_precip_auto",
        "parameters" = c("a0", "a1", "a2")
      )
    )
  }
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_precip_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a2 <- parameter_set[3, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans(repeat_a0 + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- repeat_a0[time_step, ] + (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average))
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
}





streamflow_model_precip_seasonal_ratio_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_precip_seasonal_ratio_auto",
        "parameters" = c("a0", "a1", "a2", "a4")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_precip_seasonal_ratio_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a2 <- parameter_set[3, ]
  a4 <- parameter_set[4, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans(repeat_a0 + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- repeat_a0[time_step, ] + 
      (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
      (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
      (repeat_a4[time_step, ] * repeat_seasonal_ratio[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
  
}





# Drought streamflow models ----------------------------------------------------
streamflow_model_drought_precip_only <- function(catchment_data, parameter_set) { 
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_precip_only",
        "parameters" = c("a0_d", "a0_n", "a1")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_precip_only
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  drought <- catchment_data$is_drought_year
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Model
  (repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation)
  
}




streamflow_model_drought_precip_seasonal_ratio <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_precip_seasonal_ratio",
        "parameters" = c("a0_d", "a0_n", "a1", "a4")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_precip_seasonal_ratio
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a4 <- parameter_set[4, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  drought <- catchment_data$is_drought_year
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Model
  (repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation) + (repeat_a4 * repeat_seasonal_ratio)
  
}



streamflow_model_drought_precip_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_precip_auto",
        "parameters" = c("a0_d", "a0_n", "a1", "a2")
      )
    )
  }
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_precip_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a2 <- parameter_set[4, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  drought <- catchment_data$is_drought_year
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans((repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- (repeat_a0_d[time_step, ] * repeat_drought[time_step, ]) + 
                                 (repeat_a0_n[time_step, ] * !repeat_drought[time_step, ]) + 
                                 (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
                                 (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average))
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
}




streamflow_model_drought_precip_seasonal_ratio_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_precip_seasonal_ratio_auto",
        "parameters" = c("a0_d", "a0_n", "a1", "a2", "a4")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_precip_seasonal_ratio_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a2 <- parameter_set[4, ]
  a4 <- parameter_set[5, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  drought <- catchment_data$is_drought_year
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans((repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- (repeat_a0_d[time_step, ] * repeat_drought[time_step, ]) + 
                                 (repeat_a0_n[time_step, ] * !repeat_drought[time_step, ]) +
                                 (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
                                 (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
                                 (repeat_a4[time_step, ] * repeat_seasonal_ratio[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
  
}




# Shifted CO2 models -----------------------------------------------------------
# These replace the separate only models

streamflow_model_intercept_shifted_CO2 <- function(catchment_data, parameter_set) { 

  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_intercept_shifted_CO2",
        "parameters" = c("a0", "a1", "a3_intercept", "a5")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
  streamflow_results <- catchment_data_directly_to_streamflow_model(
    catchment_data = catchment_data,
    parameter_set = parameter_set, 
    streamflow_model = streamflow_model_intercept_shifted_CO2
    )
  return(streamflow_results)
  }

  
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a3_intercept <- parameter_set[3, ]
  a5 <- parameter_set[4, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) 
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) 
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_intercept <- matrix(a3_intercept, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Model
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  repeat_a0 + (repeat_a1 * repeat_precipitation) + (repeat_a3_intercept * repeat_shifted_CO2)
  
}




streamflow_model_intercept_shifted_CO2_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_intercept_shifted_CO2_auto",
        "parameters" = c("a0", "a1", "a2", "a3_intercept", "a5")
      )
    )
  }
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_intercept_shifted_CO2_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a2 <- parameter_set[3, ]
  a3_intercept <- parameter_set[4, ]
  a5 <- parameter_set[5, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_intercept <- matrix(a3_intercept, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans(repeat_a0 + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- repeat_a0[time_step, ] + 
      (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
      (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
      (repeat_a3_intercept[time_step, ] * repeat_shifted_CO2[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
}



streamflow_model_intercept_shifted_CO2_seasonal_ratio <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_intercept_shifted_CO2_seasonal_ratio",
        "parameters" = c("a0", "a1", "a3_intercept", "a4", "a5")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_intercept_shifted_CO2_seasonal_ratio
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a3_intercept <- parameter_set[3, ]
  a4 <- parameter_set[4, ]
  a5 <- parameter_set[5, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_intercept <- matrix(a3_intercept, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Model
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  repeat_a0 + (repeat_a1 * repeat_precipitation) + (repeat_a3_intercept * repeat_shifted_CO2) + (repeat_a4 * repeat_seasonal_ratio)
  
}




streamflow_model_intercept_shifted_CO2_seasonal_ratio_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_intercept_shifted_CO2_seasonal_ratio_auto",
        "parameters" = c("a0", "a1", "a2", "a3_intercept", "a4", "a5")
      )
    )
  }
  

  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_intercept_shifted_CO2_seasonal_ratio_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a2 <- parameter_set[3, ]
  a3_intercept <- parameter_set[4, ]
  a4 <- parameter_set[5, ]
  a5 <- parameter_set[6, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_intercept <- matrix(a3_intercept, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans(repeat_a0 + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- repeat_a0[time_step, ] + 
      (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
      (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
      (repeat_a3_intercept[time_step, ] * repeat_shifted_CO2[time_step, ]) + 
      (repeat_a4[time_step, ] * repeat_seasonal_ratio[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
  
}



streamflow_model_drought_intercept_shifted_CO2 <- function(catchment_data, parameter_set) { 
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_intercept_shifted_CO2",
        "parameters" = c("a0_d", "a0_n", "a1", "a3_intercept", "a5")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_intercept_shifted_CO2
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a3_intercept <- parameter_set[4, ]
  a5 <- parameter_set[5, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  drought <- catchment_data$is_drought_year
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) 
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) 
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_intercept <- matrix(a3_intercept, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # Model
  (repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation) + (repeat_a3_intercept * repeat_shifted_CO2)
  
}



streamflow_model_drought_intercept_shifted_CO2_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_intercept_shifted_CO2_auto",
        "parameters" = c("a0_d", "a0_n", "a1", "a2", "a3_intercept", "a5")
      )
    )
  }
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_intercept_shifted_CO2_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a2 <- parameter_set[4, ]
  a3_intercept <- parameter_set[5, ]
  a5 <- parameter_set[6, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  CO2 <- catchment_data$CO2
  drought <- catchment_data$is_drought_year
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_intercept <- matrix(a3_intercept, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <-colMeans((repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- (repeat_a0_d[time_step, ] * repeat_drought[time_step, ]) + 
      (repeat_a0_n[time_step, ] * !repeat_drought[time_step, ]) + 
      (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
      (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
      (repeat_a3_intercept[time_step, ] * repeat_shifted_CO2[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
}



streamflow_model_drought_intercept_shifted_CO2_seasonal_ratio <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_intercept_shifted_CO2_seasonal_ratio",
        "parameters" = c("a0_d", "a0_n", "a1", "a3_intercept", "a4", "a5")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_intercept_shifted_CO2_seasonal_ratio
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a3_intercept <- parameter_set[4, ]
  a4 <- parameter_set[5, ]
  a5 <- parameter_set[6, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  drought <- catchment_data$is_drought_year
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_intercept <- matrix(a3_intercept, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5
  
  # Model
  (repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation) + (repeat_a3_intercept * repeat_shifted_CO2) + (repeat_a4 * repeat_seasonal_ratio)
  
}




streamflow_model_drought_intercept_shifted_CO2_seasonal_ratio_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_intercept_shifted_CO2_seasonal_ratio_auto",
        "parameters" = c("a0_d", "a0_n", "a1", "a2", "a3_intercept", "a4", "a5")
      )
    )
  }
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_intercept_shifted_CO2_seasonal_ratio_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a2 <- parameter_set[4, ]
  a3_intercept <- parameter_set[5, ]
  a4 <- parameter_set[6, ]
  a5 <- parameter_set[7, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  CO2 <- catchment_data$CO2
  drought <- catchment_data$is_drought_year
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_intercept <- matrix(a3_intercept, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans((repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- (repeat_a0_d[time_step, ] * repeat_drought[time_step, ]) + 
      (repeat_a0_n[time_step, ] * !repeat_drought[time_step, ]) + + 
      (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
      (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
      (repeat_a3_intercept[time_step, ] * repeat_shifted_CO2[time_step, ]) + 
      (repeat_a4[time_step, ] * repeat_seasonal_ratio[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
  
}



# CO2 slope streamflow models --------------------------------------------------
streamflow_model_slope_shifted_CO2 <- function(catchment_data, parameter_set) { 
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_slope_shifted_CO2",
        "parameters" = c("a0", "a1", "a3_slope", "a5")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_slope_shifted_CO2
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a3_slope <- parameter_set[3, ]
  a5 <- parameter_set[4, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  CO2 <- catchment_data$CO2
  
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_slope <- matrix(a3_slope, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # Model
  repeat_a0 + (repeat_a1 * repeat_precipitation) + (repeat_a3_slope * repeat_precipitation * repeat_shifted_CO2)
  
}




streamflow_model_slope_shifted_CO2_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_slope_shifted_CO2_auto",
        "parameters" = c("a0", "a1", "a2", "a3_slope", "a5")
      )
    )
  }
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_slope_shifted_CO2_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a2 <- parameter_set[3, ]
  a3_slope <- parameter_set[4, ]
  a5 <- parameter_set[5, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_slope <- matrix(a3_slope, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans(repeat_a0 + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- repeat_a0[time_step, ] + 
      (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
      (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
      (repeat_a3_slope[time_step, ] * repeat_precipitation[time_step, ] * repeat_shifted_CO2[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
}




streamflow_model_slope_shifted_CO2_seasonal_ratio <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_slope_shifted_CO2_seasonal_ratio",
        "parameters" = c("a0", "a1", "a3_slope", "a4", "a5")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_slope_shifted_CO2_seasonal_ratio
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a3_slope <- parameter_set[3, ]
  a4 <- parameter_set[4, ]
  a5 <- parameter_set[5, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) 
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_slope <- matrix(a3_slope, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Model
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 
  repeat_a0 + (repeat_a1 * repeat_precipitation) + (repeat_a3_slope * repeat_precipitation * repeat_shifted_CO2) + (repeat_a4 * repeat_seasonal_ratio)
  
}




streamflow_model_slope_shifted_CO2_seasonal_ratio_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_slope_shifted_CO2_seasonal_ratio_auto",
        "parameters" = c("a0", "a1", "a2", "a3_slope", "a4", "a5")
      )
    )
  }
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_slope_shifted_CO2_seasonal_ratio_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0 <- parameter_set[1, ]
  a1 <- parameter_set[2, ]
  a2 <- parameter_set[3, ]
  a3_slope <- parameter_set[4, ]
  a4 <- parameter_set[5, ]
  a5 <- parameter_set[6, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_a0 <- matrix(a0, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_slope <- matrix(a3_slope, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans(repeat_a0 + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- repeat_a0[time_step, ] + 
      (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
      (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
      (repeat_a3_slope[time_step, ] * repeat_precipitation[time_step, ] * repeat_shifted_CO2[time_step, ]) + 
      (repeat_a4[time_step, ] * repeat_seasonal_ratio[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
  
}




streamflow_model_drought_slope_shifted_CO2 <- function(catchment_data, parameter_set) { 
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_slope_shifted_CO2",
        "parameters" = c("a0_d", "a0_n", "a1", "a3_slope", "a5")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_slope_shifted_CO2
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a3_slope <- parameter_set[4, ]
  a5 <- parameter_set[5, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  drought <- catchment_data$is_drought_year
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) 
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) 
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_slope <- matrix(a3_slope, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # Model
  (repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation) + (repeat_a3_slope * repeat_precipitation * repeat_shifted_CO2)
  
}




streamflow_model_drought_slope_shifted_CO2_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_slope_shifted_CO2_auto",
        "parameters" = c("a0_d", "a0_n", "a1", "a2", "a3_slope", "a5")
      )
    )
  }
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_slope_shifted_CO2_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a2 <- parameter_set[4, ]
  a3_slope <- parameter_set[5, ]
  a5 <- parameter_set[6, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  CO2 <- catchment_data$CO2
  drought <- catchment_data$is_drought_year
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_slope <- matrix(a3_slope, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <-colMeans((repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- (repeat_a0_d[time_step, ] * repeat_drought[time_step, ]) + 
      (repeat_a0_n[time_step, ] * !repeat_drought[time_step, ]) + 
      (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
      (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
      (repeat_a3_slope[time_step, ] * repeat_precipitation[time_step, ] * repeat_shifted_CO2[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
}




streamflow_model_drought_slope_shifted_CO2_seasonal_ratio <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_slope_shifted_CO2_seasonal_ratio",
        "parameters" = c("a0_d", "a0_n", "a1", "a3_slope", "a4", "a5")
      )
    )
  } 
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_slope_shifted_CO2_seasonal_ratio
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a3_slope <- parameter_set[4, ]
  a4 <- parameter_set[5, ]
  a5 <- parameter_set[6, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  drought <- catchment_data$is_drought_year
  CO2 <- catchment_data$CO2
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_slope <- matrix(a3_slope, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5
  
  # Model
  (repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation) + (repeat_a3_slope * repeat_precipitation * repeat_shifted_CO2) + (repeat_a4 * repeat_seasonal_ratio)
  
}




streamflow_model_drought_slope_shifted_CO2_seasonal_ratio_auto <- function(catchment_data, parameter_set) {
  
  # If no parameters are given return description of model
  if(is.null(names(as.list(match.call())[-1]))) {
    return(
      list(
        "name" = "streamflow_model_drought_slope_shifted_CO2_seasonal_ratio_auto",
        "parameters" = c("a0_d", "a0_n", "a1", "a2", "a3_slope", "a4", "a5")
      )
    )
  }
  
  # Check if parameter is a matrix - if not coerce
  if(!is.matrix(parameter_set)) {
    parameter_set <- as.matrix(parameter_set, ncol = 1)
  }
  
  # If directly inputting catchment data into model then 
  # the following function will recursively call the streamflow model
  # for each list in catchment_data (start_stop)
  if(sloop::s3_class(catchment_data)[1] == "catchment_data"){ # Check type of catchment data - make this a function call for all streamflow models
    streamflow_results <- catchment_data_directly_to_streamflow_model(
      catchment_data = catchment_data,
      parameter_set = parameter_set, 
      streamflow_model = streamflow_model_drought_slope_shifted_CO2_seasonal_ratio_auto
    )
    return(streamflow_results)
  }
  
  # Parameters
  a0_d <- parameter_set[1, ]
  a0_n <- parameter_set[2, ]
  a1 <- parameter_set[3, ]
  a2 <- parameter_set[4, ]
  a3_slope <- parameter_set[5, ]
  a4 <- parameter_set[6, ]
  a5 <- parameter_set[7, ]
  
  # Get data
  precipitation <- catchment_data$precipitation
  seasonal_ratio <- catchment_data$seasonal_ratio
  CO2 <- catchment_data$CO2
  drought <- catchment_data$is_drought_year
  
  # Get into matrix form
  repeat_precipitation <- matrix(precipitation, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE) #matrix(rep(precipitation, times = ncol(parameter_set)), ncol = ncol(parameter_set), byrow = FALSE)
  repeat_seasonal_ratio <- matrix(seasonal_ratio, ncol = ncol(parameter_set), nrow = length(seasonal_ratio), byrow = FALSE)
  repeat_CO2 <- matrix(CO2, ncol = ncol(parameter_set), nrow = length(CO2), byrow = FALSE) 
  repeat_drought <- matrix(drought, ncol = ncol(parameter_set), nrow = length(precipitation), byrow = FALSE)
  repeat_a0_d <- matrix(a0_d, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a0_n <- matrix(a0_n, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a1 <- matrix(a1, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a2 <- matrix(a2, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a3_slope <- matrix(a3_slope, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a4 <- matrix(a4, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  repeat_a5 <- matrix(a5, ncol = ncol(parameter_set), nrow = nrow(repeat_precipitation), byrow = TRUE)
  
  # Autocorrelation requirements
  boxcox_streamflow_average <- colMeans((repeat_a0_d * repeat_drought) + (repeat_a0_n * !repeat_drought) + (repeat_a1 * repeat_precipitation))
  previous_boxcox_streamflow <- boxcox_streamflow_average # assume the previous flows are the average. this means the first a2 component = 0.
  
  # CO2 requirements
  repeat_shifted_CO2 <- repeat_CO2 - repeat_a5
  repeat_shifted_CO2[repeat_shifted_CO2 < 0] <- 0 # max(0, CO2 - a5)
  
  # pre-allocate
  modelled_boxcox_streamflow <- matrix(numeric(length(repeat_precipitation)), ncol = ncol(parameter_set), nrow = length(precipitation))
  
  # Model: analysis is done row by row (timestep)
  for (time_step in seq_along(precipitation)) {
    current_boxcox_streamflow <- (repeat_a0_d[time_step, ] * repeat_drought[time_step, ]) + 
      (repeat_a0_n[time_step, ] * !repeat_drought[time_step, ]) + + 
      (repeat_a1[time_step, ] * repeat_precipitation[time_step, ]) + 
      (repeat_a2[time_step, ] * (previous_boxcox_streamflow - boxcox_streamflow_average)) +
      (repeat_a3_slope[time_step, ] * repeat_precipitation[time_step, ] * repeat_shifted_CO2[time_step, ]) + 
      (repeat_a4[time_step, ] * repeat_seasonal_ratio[time_step, ])
    
    previous_boxcox_streamflow <- current_boxcox_streamflow
    modelled_boxcox_streamflow[time_step, ] <- current_boxcox_streamflow
  }
  
  return(modelled_boxcox_streamflow)
  
}




# Get streamflow_models function -----------------------------------------------
get_drought_streamflow_models <- function() {
  c(streamflow_model_drought_precip_only,
    streamflow_model_drought_precip_auto,
    streamflow_model_drought_precip_seasonal_ratio,
    streamflow_model_drought_precip_seasonal_ratio_auto,
    streamflow_model_drought_intercept_shifted_CO2,
    streamflow_model_drought_intercept_shifted_CO2_auto,
    streamflow_model_drought_intercept_shifted_CO2_seasonal_ratio,
    streamflow_model_drought_intercept_shifted_CO2_seasonal_ratio_auto,
    streamflow_model_drought_slope_shifted_CO2, 
    streamflow_model_drought_slope_shifted_CO2_auto,
    streamflow_model_drought_slope_shifted_CO2_seasonal_ratio,
    streamflow_model_drought_slope_shifted_CO2_seasonal_ratio_auto
  )
}


get_non_drought_streamflow_models <- function() {
  c(streamflow_model_precip_only,
    streamflow_model_precip_auto,
    streamflow_model_precip_seasonal_ratio,
    streamflow_model_precip_seasonal_ratio_auto,
    streamflow_model_intercept_shifted_CO2,
    streamflow_model_intercept_shifted_CO2_auto,
    streamflow_model_intercept_shifted_CO2_seasonal_ratio,
    streamflow_model_intercept_shifted_CO2_seasonal_ratio_auto,
    streamflow_model_slope_shifted_CO2,
    streamflow_model_slope_shifted_CO2_auto,
    streamflow_model_slope_shifted_CO2_seasonal_ratio,
    streamflow_model_slope_shifted_CO2_seasonal_ratio_auto
  )
}


get_all_streamflow_models <- function() {
  c(get_non_drought_streamflow_models(), get_drought_streamflow_models())
}
