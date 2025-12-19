# Produce streamflow using hist-nat rainfall
## This does not account for DREAM uncertainty - only single set of parameters


# Import libraries -------------------------------------------------------------
pacman::p_load(tidyverse, truncnorm, sloop, arrow, furrr)



### THE START STOP INDEXES ARE BROKEN ###
# I must either preserve the length in data - better more conistent 
# Dynamically calculate start_stop_indexes


# Import functions -------------------------------------------------------------
source("./Previous/Functions/utility.R")
source("./Previous/Functions/boxcox_logsinh_transforms.R")
source("./Previous/Functions/streamflow_models.R")
source("./Previous/Functions/catchment_data_blueprint.R")



# Import and prepare data ------------------------------------------------------
start_stop_indexes <- readr::read_csv(
  "./Previous/Data/start_end_index.csv",
  show_col_types = FALSE
)

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
  # Some GCM start on a cool season and miss a hot season. Produces NA - remove
  drop_na() |>
  mutate(
    # this works as expected
    standardised_warm_season_to_annual_rainfall_ratio = (warm_season / annual_scaled_hist_nat_rainfall) - mean(warm_season / annual_scaled_hist_nat_rainfall),
    .by = c(GCM, ensemble_id, gauge)
  )


## Extract high evidence ratio gauges from data ================================
data <- data |> 
  filter(gauge %in% high_evidence_ratio_gauges)


## Modify observed data tibble with GCM information ============================
### Joining data with prepared_GCM_rainfall

### Extract just the observed rainfall from data
observed_rainfall_data <- data |>
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



### To ensure same length of timeseries everything must have the same length as observed
### This ensures start_stop_index functions correctly
min_year <- observed_rainfall_data |> 
  summarise(
    min_year = min(year),
    .by = gauge
  ) |> 
  pull(min_year) |> 
  unique()
# All gauges have a start 1959


### rbind
observed_other_data <- data |>
  select(!c(p_mm, standardised_warm_season_to_annual_rainfall_ratio))


### add back the information to create catchment_data object using catchment_data_blueprint
all_data <- prepared_GCM_rainfall |>
  select(!c(warm_season, cool_season)) |>
  rbind(observed_rainfall_data) |>
  left_join(
    observed_other_data,
    by = join_by(gauge, year)
  ) |>
  # rename to work with catchment_data_blueprint
  rename(
    p_mm = annual_scaled_hist_nat_rainfall
  ) |>
  # make year an integer to make catchment_data_blueprint happy
  mutate(
    year = as.integer(year)
  ) |> 
  # set min year to ensure start_stop_index works correctly
  filter(year >= min_year)



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
unique_GCMs_ensemble_gauge_combinations <- all_data |>
  select(GCM, ensemble_id, gauge) |>
  # this must have the same gauge order as best_CO2_model_per_gauge
  distinct() |>
  unclass() |>
  unname()



### Mass catchment_data objects using GCM and observed #########################
### This make take some time...7985 combinations -> run in parallel
plan(multisession, workers = length(availableWorkers()))
catchment_data_with_GCM_and_ensemble <- future_pmap(
  .l = unique_GCMs_ensemble_gauge_combinations,
  .f = GCM_catchment_data_blueprint,
  data = all_data,
  start_stop_indexes = start_stop_indexes,
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



### Testing

### The GCM produce different rainfalls
### observed = observed rainfall
### All results show streamflow without the impact of the partitioning parameter
### TODO:
### Make graphs

testing <- open_dataset(
  source = "./Results/hist_nat_streamflow_data.parquet"
) |> 
  collect()

testing |> 
  filter(gauge == "A2390523") |> 
  ggplot(aes(x = year, y = realspace_streamflow, colour = ensemble_id)) +
  geom_line(show.legend = FALSE) +
  theme_bw() +
  facet_wrap(~GCM)


  
