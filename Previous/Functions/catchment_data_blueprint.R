# catchment_data_blueprint S3 class --------------------------------------------

## constructor =================================================================
new_catchment_data_blueprint <- function(gauge_ID, year, precipitation, observed_streamflow, is_drought_year, CO2, seasonal_ratio, start_stop_indexes) {

  ## Check types ===============================================================
  stopifnot(is.character(gauge_ID))
  stopifnot(is.integer(year))
  stopifnot(is.double(precipitation))
  stopifnot(is.double(observed_streamflow))
  stopifnot(is.logical(is_drought_year))
  stopifnot(is.double(CO2))
  stopifnot(is.double(seasonal_ratio))
  stopifnot(is.matrix(start_stop_indexes))
  
  
  
  full_data_set <- cbind(
    year, 
    precipitation, 
    observed_streamflow, 
    is_drought_year, 
    CO2, 
    seasonal_ratio) |>
    tibble::as_tibble()
  
  
  split_data_set <- purrr::map2(
    .x = start_stop_indexes[, "start_index"],
    .y = start_stop_indexes[, "end_index"],
    .f = split_full_data_set,
    full_data_set = full_data_set
  )
  
  
  ## Make class ================================================================
  structure(
    list(
      "gauge_ID" = gauge_ID,
      "contains_drought" = any(is_drought_year),
      "full_data_set" = full_data_set,
      "stop_start_data_set" = split_data_set
    ), 
    class = c("catchment_data", "list")
  ) 
}


## validator ===================================================================
validate_catchment_data_blueprint <- function(catchment_data_blueprint) {
  values <- unclass(catchment_data_blueprint)
  
  ## precipitation, observed_boxcox_streamflow, is_drought_year, CO2, seasonal_ratio all must be the same length
  
  check_length_values <- lengths(values$full_data_set) # This does not work values[c("precipitation", "observed_boxcox_streamflow", "is_drought_year", "CO2", "seasonal_ratio")]
  
  if (length(unique(check_length_values)) > 1) {
    stop(
      "precipitation, observed_streamflow, is_drought_year, CO2, seasonal_ratio all must be the same length",
      call. = FALSE
    )
  }
  
  ## if NA present in boxcox streamflow and start stop is not provided throw an error
  
  ## Start stop must have index values applicable to the precipitation... vectors
  
  # is the dataset empty?
  if (is_empty_tibble(values$full_data_set)) {
    stop(
      "The tibble containing data is empty. Maybe the gauge_ID is not in the observed data."
    )
  }
  
  # check if start_stop tibbles are empty
  browser()
  empty_start_stop <- map_lgl(
    .x = values$stop_start_data_set,
    .f = is_empty_tibble
  ) |> 
    any()
  
  if(empty_start_stop) {
    stop(
      "At least one tibble in start_stop_index is empty. Check start stop indices."
    )
  }
  
  # stop return message
  
  # Invisibly return input
  catchment_data_blueprint
}



## helper ======================================================================
catchment_data_blueprint <- function(gauge_ID, observed_data, start_stop_indexes, ...) {
  
  force(gauge_ID)
  # Two options:
  # 1. provide all variables individually i.e., year, precip etc
  # 2. just provide observed data with variables for extraction
  
  
  ## Option 1 ==================================================================
  # If observed_data is null the user must enter gauge_ID, year, etc. manually
  if (is.null(observed_data)) {
    new_catchment_data_blueprint(
      gauge_ID = gauge_ID,
      year = year,
      precipitation = precipitation,
      observed_streamflow = observed_streamflow,
      is_drought_year = is_drought_year,
      CO2 = CO2,
      seasonal_ratio = standardised_warm_season_to_annual_rainfall_ratio,
      start_stop_indexes = single_gauge_start_stop_indexes
    ) |>
      validate_catchment_data_blueprint()
  }
  
  
  ## Option 2 ================================================================== 
  
  # Split up data based on start and stop indexes
  single_gauge_observed_data <- observed_data |> 
    dplyr::filter(gauge == {{ gauge_ID }})
  
  single_gauge_start_stop_indexes <- start_stop_indexes |> 
    dplyr::filter(gauge == {{ gauge_ID }}) |> 
    dplyr::select(!gauge) |> 
    as.matrix()
  
  new_catchment_data_blueprint(
    gauge_ID = {{ gauge_ID }},
    year = single_gauge_observed_data$year,
    precipitation = single_gauge_observed_data$p_mm,
    observed_streamflow = single_gauge_observed_data$q_mm,
    is_drought_year = single_gauge_observed_data$drought,
    CO2 = single_gauge_observed_data$CO2,
    seasonal_ratio = single_gauge_observed_data$standardised_warm_season_to_annual_rainfall_ratio,
    start_stop_indexes = single_gauge_start_stop_indexes
  ) |>
    validate_catchment_data_blueprint()
  
}





## Assoicated functions ========================================================
## Split full data set into smaller tables =====================================
split_full_data_set <- function(start_index, stop_index, full_data_set) {
  
  full_data_set |> 
    dplyr::slice(start_index:stop_index)
  
}



## Get the observed_boxcox_streamflow from split full data set =================
pull_vector_from_split_data_set <- function(split_data_set, column_name) {
  
  split_data_set |> 
    dplyr::pull({{  column_name }})
  
}



