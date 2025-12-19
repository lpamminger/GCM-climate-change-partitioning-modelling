# Creates an index table of start index and end index with removed NA ----------

continuous_run_start_end <- function(input_vector) {
  
  # Based off the code Tom gave me
  # requires checkmate for wf and wl
  
  NA_is_true <- !is.na(input_vector) # logicals of NA
  
  # Coherses logical to numeric. 
  start_end_marking <- NA_is_true[2:NROW(NA_is_true)] - NA_is_true[1:(NROW(NA_is_true) - 1)] #Makes start of NA -1 and end 1
  
  if(NA_is_true[1] == TRUE) {
    start_index <- c(checkmate::wf(NA_is_true, TRUE), (which(start_end_marking == 1) + 1)) # wf is which_first
  } else {
    start_index <- which(start_end_marking == 1) + 1
  }
  
  if(NA_is_true[NROW(NA_is_true)] == TRUE) {
    end_index <- c(which(start_end_marking == -1), checkmate::wl(NA_is_true, TRUE)) # wl is which_last
    
  } else {
    end_index <- which(start_end_marking == -1)
  }
  
  cbind(start_index, end_index)
} 



# get_date() returns current date as YYYYMMDD ----------------------------------
get_date <- function() {
  str_remove_all(Sys.Date(), "-")
}


# Find the maximum run without NA in a vector ----------------------------------
max_continuous_run_start_end <- function(input_vector) {
  
  # Based off the code Tom gave me
  # requires checkmate for wf and wl
  
  NA_is_true <- !is.na(input_vector) # logicals of NA
  
  # Coherses logical to numeric. 
  start_end_marking <- NA_is_true[2:NROW(NA_is_true)] - NA_is_true[1:(NROW(NA_is_true) - 1)] #Makes start of NA -1 and end 1
  
  if(NA_is_true[1] == TRUE) {
    start_index <- c(checkmate::wf(NA_is_true, TRUE), (which(start_end_marking == 1) + 1)) # wf is which_first
  } else {
    start_index <- which(start_end_marking == 1) + 1
  }
  
  if(NA_is_true[NROW(NA_is_true)] == TRUE) {
    end_index <- c(which(start_end_marking == -1), checkmate::wl(NA_is_true, TRUE)) # wl is which_last
    
  } else {
    end_index <- which(start_end_marking == -1)
  }
  
  max_run_index <- which.max(end_index - start_index)
  
  c(start_index[max_run_index], end_index[max_run_index])
  
} 



# rle that can deal with NA values ---------------------------------------------

# Code from : https://coolbutuseless.github.io/2020/08/26/run-length-encoding-and-the-problem-of-nas/
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#' A drop-in replacement for code{base::rle()} that treats all NAs as identical
#'
#' param x an atomic vector
#'
#' return An object of class code{rle}
#'
#' export
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
rle2 <- function (x)  {
  stopifnot("'x' must be a vector of an atomic type" = is.atomic(x))
  
  n <- length(x)
  if (n == 0L) {
    return(structure(list(
      lengths = integer(), values = x)
    ), class = 'rle')
  }
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Where does next value not equal current value?
  # i.e. y will be TRUE at the last index before a change
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  y <- (x[-1L] != x[-n])
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Since NAs are never equal to anything, NAs in 'x' will lead to NAs in 'y'.
  # These current NAs in 'y' tell use nothing - Set all NAs in y to FALSE
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  y[is.na(y)] <- FALSE
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # When a value in x is NA and its successor is not (or vice versa) then that
  # should also count as a value change and location in 'y' should be set to TRUE
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  y <- y | xor(is.na(x[-1L]), is.na(x[-n]))
  
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Any TRUE locations in 'y' are the end of a run, where the next value
  # changes to something different
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  i <- c(which(y), n)
  
  structure(list(
    lengths = diff(c(0L, i)),
    values  = x[i]
  ), class = 'rle')
}



# rolling
rollapply <- function(x, n, f, ...) {
  
  offset <- trunc(n - 1)
  out <- rep(NA, length(x) - offset)
  
  for (i in (offset + 1):length(x)) {
    out[i] <- f(x[(i - offset):i], ...)
  }
  
  return(out)
}



# Round based on function and provide accuracy ---------------------------------
round_any = function(x, accuracy, f = round){
  f(x / accuracy) * accuracy
}



# Saft 2015 drought_algorithm --------------------------------------------------
# input is rainfall data
# output is a logical vector containing true if drought year and false if not

saft_drought_algorithm <- function(rainfall_data){
  
  # There must be more than 2 years of rainfall_data
  # Otherwise, return NA
  if (length(rainfall_data) <= 1){
    return(NA)
  }
  
  # Calculate rainfall anomaly -------------------------------------------------
  mean_rainfall <- mean(rainfall_data)
  anomaly_rainfall <- (rainfall_data - mean_rainfall) / mean_rainfall
  
  # Apply a 3-year mean moving window to smooth out rainfall
  WINDOW <- 3
  smooth_anomaly_rainfall <- rollapply(anomaly_rainfall, WINDOW, mean)
  
  # Find when the dry periods are ----------------------------------------------
  ## A dry period occurs when:
  ###  - mean dry period anomaly < -0.05 (5 %)
  ### - and length >= 7 years
  dry_years <- rle(smooth_anomaly_rainfall < -0.05) # Filter years below anomaly
  dry_years$values[dry_years$lengths < 7] = FALSE # If any run is < 7 set it to non-dry year (FALSE)
  drought <- inverse.rle(dry_years) # Return TRUE/FALSE vector with indicating drought
  
  # Saft has specific criteria for ending droughts -----------------------------
  adjusted_drought <- drought
  drought_rle <- rle(drought)
  drought_end_year_index <- cumsum(drought_rle$lengths)[drought_rle$values == TRUE]
  
  for (drought_end_year in seq_along(drought_end_year_index)){ # probably could vapply this
    
    # Case 1 and 2 are mutually exclusive for a three year window. 
    # For windows larger than 3 they could both occur
    
    # Case 1: last two years are positive and less than 0.15 --> all(double < 0.15 & double > 0)
    last_two_years <- anomaly_rainfall[(drought_end_year_index[drought_end_year]-1):drought_end_year_index[drought_end_year]]
    if (all(last_two_years < 0.15 & last_two_years > 0)) { # Only have to check last two elements
      adjusted_drought[(drought_end_year_index[drought_end_year]-1):drought_end_year_index[drought_end_year]] = c(TRUE, FALSE) 
    }
    
    # Case 2: Check if any year is > 0.15. If so end the drought a 
    # year before the 0.15 occurrence
    
    check_last_window <- anomaly_rainfall[(drought_end_year_index[drought_end_year]-(WINDOW-1)):drought_end_year_index[drought_end_year]]
    rev_window_iter <- rev(seq_along(check_last_window))
    
    for (window_iter in seq_along(check_last_window)){
      true_position <- check_last_window[window_iter] > 0.15
      
      if (true_position) {
        adjusted_drought[drought_end_year_index[drought_end_year] - rev_window_iter[window_iter]] <- TRUE 
        adjusted_drought[(drought_end_year_index[drought_end_year] - rev_window_iter[window_iter] + 1):drought_end_year_index[drought_end_year]] <- FALSE
        break
      } 
    }
  }
  
  return(adjusted_drought)
}


# Copying histogram bin function factory from advanced R 2e (put in separate folder)
binwidth_bins <- function(n){
  force(n)
  
  function(x){
    (max(x) - min(x)) / n
  }
}


# Put a variable in an get its variable name as a character
convert_object_variable_to_character <- function(variable) {
  as.character(substitute(variable))
}


# find the order of magnitude of a number
get_order_magnitude <- function(x) {
  get_sign <- sign(x)
  get_sign * 10^(floor(log10(abs(x))))
}




# generate map of australia shapefile
generate_aus_map_sf <- function() {
  
  # Requires ozmaps package
  
  aus_map <- ozmaps::ozmap(x = "states", add = FALSE) |>
    filter(!NAME %in% c("Other Territories")) |>
    rename(state = NAME) |>
    mutate(
      state = case_when(
        state == "New South Wales" ~ "NSW",
        state == "Victoria" ~ "VIC",
        state == "Queensland" ~ "QLD",
        state == "South Australia" ~ "SA",
        state == "Western Australia" ~ "WA",
        state == "Tasmania" ~ "TAS",
        state == "Northern Territory" ~ "NT",
        state == "Australian Capital Territory" ~ "ACT",
      )
    )
  
  
  combine_NSW_ACT <- aus_map |>
    filter(state %in% c("NSW", "ACT")) |>
    st_union()
  
  
  aus_map[1, 2] <- list(combine_NSW_ACT)
  
  
  aus_map <- aus_map |>
    filter(state != "ACT")
  
  return(aus_map)
}



is_empty_tibble <- function(x) {
  if_else(nrow(x) * ncol(x) == 0, TRUE, FALSE)
}
