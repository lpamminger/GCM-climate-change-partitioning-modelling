# Produce graphs using hist_nat_streamflow results -----------------------------


# Import libraries -------------------------------------------------------------
pacman::p_load(tidyverse, truncnorm, sloop, arrow, furrr)


# Import data ------------------------------------------------------------------
## Data set contains GCM = observed and ensemble_id = observed 
### --> this uses observed rainfall with the CO2 component turned off
hist_nat_streamflow_data <- open_dataset(
  source = "./Results/hist_nat_streamflow_data.parquet"
) |> 
  collect()

## Streamflow using observed precipitation with CO2 turned on
### streamflow_cmaes.csv import here


# GCM meta information ---------------------------------------------------------
GCM_meta_info <- hist_nat_streamflow_data |> 
  select(GCM, ensemble_id) |> 
  distinct() |> 
  summarise(
    n = n(),
    .by = GCM
  )



### IDEAS:
### TODO:
### Make graphs
#### - get ensemble median for a given GCM
#### - get GCM median for a given gauge
#### - plot streamflow with error bands



hist_nat_streamflow_data |> 
  filter(gauge == "138004B") |> 
  ggplot(aes(x = year, y = realspace_streamflow, colour = ensemble_id)) +
  geom_line(show.legend = FALSE) +
  theme_bw() +
  facet_wrap(~GCM)
