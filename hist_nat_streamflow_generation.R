# Produce streamflow using hist-nat rainfall
## This does not account for DREAM uncertainty - only single set of parameters


# Import libraries -------------------------------------------------------------
pacman::p_load(tidyverse, truncnorm, sloop, arrow, furrr)



# Import functions -------------------------------------------------------------
source("./Previous/Functions/utility.R")
source("./Previous/Functions/boxcox_logsinh_transforms.R")
source("./Previous/Functions/streamflow_models.R")
source("./Previous/Functions/catchment_data_blueprint.R")



# Import and prepare data ------------------------------------------------------

data <- readr::read_csv( # data will be in the package
  "./Previous/Data/with_NA_yearly_data_CAMELS.csv",
  show_col_types = FALSE
) |>
  mutate(
    year = as.integer(year)
  ) |>
  # required for log-sinh. Log-sinh current formulation has asymptote of zero.
  # This means zero flows of ephemeral catchments cannot be transformed
  # add a really small value
  mutate(q_mm = q_mm + .Machine$double.eps^0.5)


gauge_information <- readr::read_csv(
  "./Previous/Data/gauge_information_CAMELS.csv",
  show_col_types = FALSE
)

best_CO2_non_CO2_per_gauge <- read_csv(
  "./Previous/Results/best_CO2_non_CO2_per_catchment_CMAES.csv",
  show_col_types = FALSE
)


GCM_rainfall <- open_dataset(
  "./Results/scale_term/scaled_hist_nat_rainfall.parquet"
)


# Regenerate the streamflow using data and best_CO2_non_CO2_per_gauge ----------

## Extract gauges with evidence ratio > 100 ====================================
high_evidence_ratio_gauges <- best_CO2_non_CO2_per_gauge |>
  select(gauge, contains_CO2, AIC) |>
  distinct() |>
  pivot_wider(
    names_from = contains_CO2,
    values_from = AIC
  ) |>
  mutate(
    CO2_model = `TRUE`,
    non_CO2_model = `FALSE`,
    .keep = "unused"
  ) |>
  mutate(
    AIC_difference = CO2_model - non_CO2_model # CO2 is smaller than non-CO2 then negative and CO2 is better
  ) |>
  mutate(
    evidence_ratio = case_when(
      AIC_difference < 0 ~ exp(0.5 * abs(AIC_difference)), # when CO2 model is better
      AIC_difference > 0 ~ -exp(0.5 * abs(AIC_difference)) # when non-CO2 model is better
    )
  ) |>
  arrange(evidence_ratio) |>
  filter(evidence_ratio > 100) |> # moderately strong > 100, moderate is > 10
  pull(gauge)




## Prepare GCM rainfall data ===================================================
prepared_GCM_rainfall <- GCM_rainfall |>
  select(!c(historical, hist_nat, smooth_scale_term, realisation)) |>
  filter(gauge %in% high_evidence_ratio_gauges) |>
  collect() |>
  pivot_wider(
    id_cols = c(year, GCM, ensemble_id, gauge),
    names_from = season,
    values_from = smooth_hist_nat
  ) |>
  mutate(
    annual_scaled_hist_nat_rainfall = warm_season + cool_season
  ) |>
  mutate(
    # this works as expected
    standardised_warm_season_to_annual_rainfall_ratio = (warm_season / annual_scaled_hist_nat_rainfall) - mean(warm_season / annual_scaled_hist_nat_rainfall, na.rm = T),
    .by = c(GCM, ensemble_id, gauge)
  )





## Prepare observed data =======================================================
data <- data |> 
  filter(gauge %in% high_evidence_ratio_gauges)
# there needs to be a check here for gauges with ToE> 2014


## Compare observed data to GCM data ===========================================
observed_start_end_years <- data |> 
  summarise(
    obs_start_year = min(year),
    obs_end_year = max(year),
    .by = gauge
  ) 

for_filtering_observed_start_end_years <- observed_start_end_years |> 
  rename(
    GCM_start_year = obs_start_year,
    GCM_end_year = obs_end_year
  ) |> 
  add_column(
    GCM = "observed",
    ensemble_id = "observed",
    .after = 1
  )

### Check GCMs start and end dates - compare with observed
GCM_start_end_years <- prepared_GCM_rainfall |> 
  summarise(
    GCM_start_year = min(year),
    GCM_end_year = max(year),
    .by = c(gauge, GCM, ensemble_id)
  )


### Combine GCM and observed start and end year
sort_GCMs <- GCM_start_end_years |> 
  left_join(
    observed_start_end_years,
    by = join_by(gauge)
  ) |> 
  # Check 1 - remove GCM that do not have data in the observed period
  filter(
    GCM_end_year > obs_start_year
  ) |> 
  # Check 2 - GCM start year must be >= 1959 for start stop index
  filter(
    GCM_start_year <= 1959
  ) |> 
  # Check 3 - GCM end year must be 2014 all entries 
  filter(
    GCM_end_year == 2014
  ) |> 
  # Check 4 - add observed to GCM and ensemble for filtering later
  select(!c(obs_start_year, obs_end_year)) |> 
  rbind(for_filtering_observed_start_end_years) |> 
  arrange(desc(GCM_start_year))


### Results show:
### - observed starts during 1959 for all entries
### - GCM ends during 2014 for all entries expect for 1 GCM - cut at 2014
### - GCMs have missing data - account for this
### This is problematic using the existing start stop indexes csv as 
### last entries > 2014 will be references non-existent data

# To ensure start_stop can be used for observed and GCM shorten observed to
# < 2014 and redo start stop.
# In future it would be a good idea to have the start stop built into the 
# catchment_data_set_blueprint

## New data ====================================================================
short_data <- data |> 
  filter(year <= 2014)


## New start stop ==============================================================
gauge_continous_start_end <- function(single_gauge, data, min_run_length) {
  
  gauge_specific_data <- data |>
    filter(gauge == {{ single_gauge }})
  
  start_end_matrix <- continuous_run_start_end(gauge_specific_data$q_mm) # apply it to every gauge.
  
  start_end_matrix |>
    as_tibble() |>
    mutate(length = end_index - start_index + 1) |> # + 1 to make the input more easy to understand
    filter(length > min_run_length - 1) |>  
    add_column(
      "gauge" = {{ single_gauge }}, 
      .before = 1
      )
} 


start_stop_indexes <- map(
  .x = data |> pull(gauge) |> unique(), 
  .f = gauge_continous_start_end,
  data = short_data,
  min_run_length = 2L
) |> 
  list_rbind()





## Join observed and GCM rainfall into single dataset ==========================
## I can't figure out how to join them so I:
### 1. extract just observed_rainfall and give it GCM/ensemble of observed
### 2. rbind() it to the GCM dataset
### 3. Left join other observed_data to GCM_dataset

### 1. Extract just the observed rainfall from data ############################
observed_rainfall_data <- short_data |>
  select(year, gauge, p_mm, standardised_warm_season_to_annual_rainfall_ratio) |>
  # rename to match prepared_GCM_rainfall
  rename(
    annual_scaled_hist_nat_rainfall = p_mm
  ) |>
  # add GCM and ensemble_id
  add_column(
    GCM = "observed",
    ensemble_id = "observed"
  )


### 2. rbind() it to the GCM dataset ###########################################
observed_and_GCM_rainfall <- prepared_GCM_rainfall |> 
  select(!c(warm_season, cool_season)) |> 
  rbind(observed_rainfall_data)


### 3. Left join other observed_data to GCM_dataset ############################
observed_other_data <- short_data |>
  select(!c(p_mm, standardised_warm_season_to_annual_rainfall_ratio))

all_data <- observed_and_GCM_rainfall |>
  left_join(
    observed_other_data,
    by = join_by(gauge, year)
  ) 


## Clean up all-data using previous findings ===================================
clean_all_data <- all_data |> 
  # rename to work with catchment_data_blueprint
  rename(
    p_mm = annual_scaled_hist_nat_rainfall
  ) |> 
  # convert year to integer for catchment_data_blueprint
  mutate(year = as.integer(year)) |>  
  # Filter out GCM without data in observed period
  semi_join(
    sort_GCMs,
    by = join_by(gauge, GCM, ensemble_id)
  ) |> 
  # Minimum year for start-stop index
  filter(year >= 1959) |> 
  arrange(year, gauge, GCM, ensemble_id)


### Final check - make sure GCM data is the same length as observed ############
obs_count <- clean_all_data |> 
  filter(GCM == "observed") |> 
  summarise(
    obs_count = n(),
    .by = gauge
  )

GCM_ensemble_count <- clean_all_data |> 
  filter(GCM != "observed") |> 
  summarise(
    count = n(),
    .by = c(GCM, ensemble_id, gauge)
  ) |> 
  left_join(
    obs_count,
    by = join_by(gauge)
  ) |> 
  mutate(
    check = count == obs_count
  ) |> 
  filter(!check) |> 
  # this is to exlude those catchment with lots of missing data
  filter(count < 50)

# observed data mismatch for gauge (missing obs year - not sure why) 
## - 216002 (1961, 1963), 
## - 227213 (1963), 
## - 235205 (1978), 
## - 415223 (1981) 

#clean_all_data |> 
#  filter(GCM == "observed") |> 
#  filter(gauge == "415223") |> 
#  pull(year)

## manually remove these entires in the observed
manual_removal_obs <- tribble(
  ~year,  ~gauge,
  1961, "216002",
  1963, "216002",
  1978, "235205",
  1963, "227213",
  1981, "415223",
  1986, "415223"
)


# Cases when this occurs:
## - mismatch of data between hist and hist nat
clean_all_data <- clean_all_data |> 
  anti_join(
    GCM_ensemble_count,
    by = join_by(GCM, ensemble_id, gauge)
  ) |> 
  anti_join(
    manual_removal_obs,
    by = join_by(year, gauge)
  )

# manually changing value requires an update to start stop index
update_start_stop_data <- clean_all_data |> 
  filter(GCM == "observed") 


updated_start_stop_indexes <- map(
  .x = update_start_stop_data |> pull(gauge) |> unique(), 
  .f = gauge_continous_start_end,
  data = update_start_stop_data,
  min_run_length = 2L
) |> 
  list_rbind()


## Build catchment_data_set objects ============================================
### wrapper around catchment data blueprint to include GCM information

GCM_catchment_data_blueprint <- function(GCM, ensemble_id, gauge_ID, data, start_stop_indexes) {
  filtered_observed_data <- data |>
    filter(GCM == {{ GCM }}) |>
    filter(ensemble_id == {{ ensemble_id }}) |> 
    filter(gauge == {{ gauge_ID }})

  
  catchment_data_result <- catchment_data_blueprint(
      gauge_ID = gauge_ID,
      observed_data = filtered_observed_data,
      start_stop_indexes = start_stop_indexes
  )
    
  # modify the class directly
  catchment_data_result$GCM <- GCM
  catchment_data_result$ensemble_id <- ensemble_id
    
  return(catchment_data_result)
}


### Get unique GCM, ensemble_id and gauge_ID vectors for iteration
unique_GCMs_ensemble_gauge_combinations <- clean_all_data |>
  select(GCM, ensemble_id, gauge) |>
  # this must have the same gauge order as best_CO2_model_per_gauge
  distinct() |>
  unclass() |>
  unname()

#x <- GCM_catchment_data_blueprint(
#  GCM = "ACCESS-CM2",
#  ensemble_id = "r1i1p1f1",
#  gauge_ID = "415223",
#  data = clean_all_data,
#  start_stop_indexes = updated_start_stop_indexes
#)$stop_start_data_set$start_index


### Mass catchment_data objects using GCM and observed #########################
### This make take some time...7985 combinations -> run in parallel
plan(multisession, workers = length(availableWorkers()))
catchment_data_with_GCM_and_ensemble <- future_pmap(
  .l = unique_GCMs_ensemble_gauge_combinations,
  .f = GCM_catchment_data_blueprint,
  data = clean_all_data,
  start_stop_indexes = updated_start_stop_indexes,
  .options = furrr_options(seed = 1L),
  .progress = TRUE
)


## Extract streamflow function from best_CO2_model_gauge =======================

### Filtered gauge, model and parameters #######################################
best_CO2_model_with_params_per_gauge <- best_CO2_non_CO2_per_gauge |>
  filter(gauge %in% high_evidence_ratio_gauges) |>
  # remove non-CO2 models
  filter(contains_CO2) #|> 
  # this must have the same gauge order as the catchment_data
  #arrange(match(gauge, unique_GCMs_ensemble_gauge_combinations[[3]]))


best_CO2_model_per_gauge <- best_CO2_model_with_params_per_gauge |>
  select(gauge, streamflow_model) |>
  distinct() 

### Match number of items catchment_data_with_GCM_and_ensemble for pmap
repeated_best_CO2_per_gauge <- unique_GCMs_ensemble_gauge_combinations |> 
  `names<-`(c("GCM", "ensemble_id", "gauge")) |> 
  as_tibble() |> 
  left_join(
    best_CO2_model_per_gauge,
    by = join_by(gauge)
  )

### match.fun can call a function using the character string of the function name
extracted_models <- map(
  .x = repeated_best_CO2_per_gauge |> pull(streamflow_model),
  .f = match.fun
)




## Extract parameter set =======================================================
extract_params_from_results <- function(GCM, ensemble_id, gauge, results) {
results |>
    filter(GCM == {{ GCM }}) |> 
    filter(ensemble_id == {{ ensemble_id }}) |> 
    filter(gauge == {{ gauge }}) |>
    pull(parameter_value)
}


modified_a3_best_CO2_model_with_params_per_gauge <- best_CO2_model_with_params_per_gauge |>
  mutate(
    parameter_value = if_else(str_detect(parameter, "a3"), 0, parameter_value)
  )

repeated_modified_a3_best_CO2_model_with_params_per_gauge <- unique_GCMs_ensemble_gauge_combinations |> 
  `names<-`(c("GCM", "ensemble_id", "gauge")) |> 
  as_tibble() |> 
  left_join(
    modified_a3_best_CO2_model_with_params_per_gauge,
    by = join_by(gauge),
    relationship = "many-to-many"
  ) 
  
extracted_params <- pmap(
  .l = unique_GCMs_ensemble_gauge_combinations, # this should have the same gauge order as unique_GCMs_ensemble_gauge_combinations[[3]]
  .f = extract_params_from_results,
  results = repeated_modified_a3_best_CO2_model_with_params_per_gauge
)






## Regenerate streamflow (log-sinh space) ======================================
regenerate_transformed_streamflow <- function(catchment_data, extracted_parameters, extracted_model) {
  
  # makeshift breakpoint for misbehaving catchments
  #if(catchment_data$gauge == "415223") {#415223, 235205, 227213
  #  browser() 
  #}
  
  results <- extracted_model(
    catchment_data = catchment_data,
    parameter_set = extracted_parameters
  )

  ### The second last parameter is always the log-sinh transform
  log_sinh_parameter_index <- length(extracted_parameters) - 1
  log_sinh_parameter <- extracted_parameters[log_sinh_parameter_index]


  results |>
    rename(transformed_streamflow = streamflow_results) |>
    ### Add realspace streamflow to results
    mutate(
      realspace_streamflow = inverse_log_sinh_transform(b = log_sinh_parameter, z = transformed_streamflow, offset = 0)
    ) |>
    ### Add gauge ID to results
    mutate(
      gauge = catchment_data$gauge_ID,
      GCM = catchment_data$GCM,
      ensemble_id = catchment_data$ensemble_id,
      .before = 1
    )
}


### final result ###############################################################
regenerated_streamflow_data <- pmap(
  .l = list(catchment_data_with_GCM_and_ensemble, extracted_params, extracted_models),
  .f = regenerate_transformed_streamflow
) |>
  list_rbind() |> 
  arrange(gauge)
  

write_parquet(
  regenerated_streamflow_data,
  sink = "./Results/hist_nat_streamflow_data.parquet"
)





  
