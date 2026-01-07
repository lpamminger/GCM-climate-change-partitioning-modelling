# Produce graphs using hist_nat_streamflow results -----------------------------


# Import libraries -------------------------------------------------------------
pacman::p_load(tidyverse, truncnorm, sloop, arrow, furrr, ozmaps, sf, ggmagnify, ggplot2, patchwork)


# Import functions -------------------------------------------------------------
source("Previous/Functions/utility.R")

# Import data ------------------------------------------------------------------

## lat lon state data ==========================================================
lat_lon_data <- read_csv(
  "Previous/Data/gauge_information_CAMELS.csv",
  show_col_types = FALSE
  ) |> 
  select(gauge, lat, lon, state)

## hist nat streamflow =========================================================
## Data set contains GCM = observed and ensemble_id = observed
### --> this uses observed rainfall with the CO2 component turned off

### Remove gauges with a time of activation > 2014
### HARD CODED FROM RQ2
gauge_ToE_greater_than_2014 <- c("401210", "410156", "411003", "603007", "204036", "224213")

hist_nat_streamflow_data <- open_dataset(
  source = "./Results/hist_nat_streamflow_data.parquet"
) |>
  collect() |>
  filter(!gauge %in% gauge_ToE_greater_than_2014)


## cmaes streamflow ============================================================
high_evidence_ratio_gauges <- hist_nat_streamflow_data |>
  pull(gauge) |>
  unique()

best_CO2_model_per_catchment_CMAES <- read_csv(
  "Previous/Results/best_CO2_non_CO2_per_catchment_CMAES.csv",
  show_col_types = FALSE
) |>
  filter(contains_CO2) |>
  filter(gauge %in% high_evidence_ratio_gauges) |>
  select(gauge, streamflow_model)

cmaes_streamflow <- read_csv(
  "./Previous/Results/cmaes_streamflow_results.csv",
  show_col_types = FALSE
) |>
  semi_join(
    best_CO2_model_per_catchment_CMAES,
    by = join_by(gauge, streamflow_model)
  ) |>
  filter(!gauge %in% gauge_ToE_greater_than_2014)



# GCM meta information ---------------------------------------------------------
GCM_meta_info <- hist_nat_streamflow_data |>
  select(GCM, ensemble_id) |>
  distinct() |>
  summarise(
    n = n(),
    .by = GCM
  )



# Comparing streamflow timeseries ----------------------------------------------

## hist_nat ====================================================================
hist_nat_plotting_data <- hist_nat_streamflow_data |>
  filter(GCM != "observed") |>
  # Ensemble median per GCM
  summarise(
    median_ensemble_realspace_streamflow = median(realspace_streamflow),
    .by = c(year, GCM, gauge)
  ) |>
  # GCM median
  summarise(
    median_GCM_realspace_streamflow = median(median_ensemble_realspace_streamflow),
    max_GCM_realspace_streamflow = max(median_ensemble_realspace_streamflow),
    min_GCM_realspace_streamflow = min(median_ensemble_realspace_streamflow),
    .by = c(year, gauge)
  ) |>
  add_column(
    type = "Counterfactual - Hist Nat Precipitation"
  )

## obs precip CO2 off ==========================================================
obs_CO2_off_plotting_data <- hist_nat_streamflow_data |>
  filter(GCM == "observed") |>
  select(year, gauge, realspace_streamflow) |>
  rename(median_GCM_realspace_streamflow = realspace_streamflow) |>
  add_column(
    max_GCM_realspace_streamflow = NA,
    min_GCM_realspace_streamflow = NA,
    type = "Counterfactual - Observed Precipitation"
  )

# obs precip CO2 on ============================================================
obs_CO2_on_plotting_data <- cmaes_streamflow |>
  # GCM data stops at 2014 - make end year same
  filter(year <= 2014) |>
  select(year, gauge, realspace_modelled_streamflow) |>
  rename(median_GCM_realspace_streamflow = realspace_modelled_streamflow) |>
  add_column(
    max_GCM_realspace_streamflow = NA,
    min_GCM_realspace_streamflow = NA,
    type = "CO2 Model - Observed Precipitation"
  )


## join them up for plotting ===================================================
all_plotting_data <- rbind(hist_nat_plotting_data, obs_CO2_off_plotting_data, obs_CO2_on_plotting_data) |>
  # factor for order
  mutate(
    type = factor(type, levels = c("CO2 Model - Observed Precipitation", "Counterfactual - Observed Precipitation", "Counterfactual - Hist Nat Precipitation"))
  )

## plot and save ===============================================================
plot <- all_plotting_data |>
  ggplot(aes(x = year, y = median_GCM_realspace_streamflow, colour = type, fill = type)) +
  geom_ribbon(
    aes(x = year, ymin = min_GCM_realspace_streamflow, ymax = max_GCM_realspace_streamflow),
    alpha = 0.2,
    inherit.aes = FALSE,
    data = hist_nat_plotting_data,
    fill = "#7570b3"
  ) +
  geom_line() +
  theme_bw() +
  labs(
    y = "Streamflow (mm)",
    x = "Year",
    colour = "Modelled Streamflow Method"
  ) +
  scale_colour_brewer(palette = "Dark2") +
  facet_wrap(~gauge, scales = "free_y")


ggsave(
  file = "modelled_streamflow_timeseries.pdf",
  path = "./Figures",
  plot = plot,
  device = "pdf",
  width = 1189,
  height = 841,
  units = "mm"
)


# Decompose of rainfall and CO2 partitioning on streamflow ---------------------

## Ignore rainfall uncertainty for now - can I just use min/max for the uncertainty?
## The pivot wider angle with uncertainty will be complex
## Approach:
## - get `counterfactual hist nat precipitation` streamflow for all GCMs
## - find the relative impact of of `counterfactual hist nat precipitation` on streamflow for all GCMs
## - use the range or IQR of the range of values of the relative impact


## Decompose each impact at each timestep ======================================

## Total effect = `Counterfactual - Hist Nat Precipitation` - `CO2 Model - Observed Precipitation`  
## Rainfall effect = `Counterfactual - Hist Nat Precipitation` - `Counterfactual - Observed Precipitation`
## Partitioning effect = `Counterfactual - Observed Precipitation` - `CO2 Model - Observed Precipitation` 

decomposing_impacts <- all_plotting_data |> 
  select(!c(max_GCM_realspace_streamflow, min_GCM_realspace_streamflow)) |> 
  pivot_wider(
    id_cols = c(year, gauge),
    names_from = type,
    values_from = median_GCM_realspace_streamflow
  ) |>
  drop_na() 
  # decompose using formula above
  #mutate(
  #  total_effect = `Counterfactual - Hist Nat Precipitation` - `CO2 Model - Observed Precipitation`,
  #  rainfall_effect = `Counterfactual - Hist Nat Precipitation` - `Counterfactual - Observed Precipitation`,
  #  partitioning_effect = `Counterfactual - Observed Precipitation` - `CO2 Model - Observed Precipitation` 
  #) |> 
  # find relative impact
  #mutate(
  #  relative_rainfall_effect = rainfall_effect / total_effect,
  #  relative_partitioning_effect = partitioning_effect / total_effect
  #)


## Aggregate impacts on two specific decades ===================================
### Do the calcuation on the total rather than year specific average
decade_1 <- seq(from = 1990, to = 1999)
decade_2 <- seq(from = 2012, to = 2021)

decade_specific_decomposed_impacts <- decomposing_impacts |> 
  filter(year %in% c(decade_1, decade_2)) |> 
  # add decade column
  mutate(
    decade = case_when(
      year %in% decade_1 ~ 1,
      year %in% decade_2 ~ 2,
      .default = NA
    )
  ) |> 
  # aggregate by decade
  summarise(
    sum_counterfactual_hist_nat = sum(`Counterfactual - Hist Nat Precipitation`),
    sum_counterfactual_obs = sum(`Counterfactual - Observed Precipitation`),
    sum_CO2_obs = sum(`CO2 Model - Observed Precipitation`),
    .by = c(gauge, decade)
  ) |> 
  # decompose - take abs two catchments CO2 increases streamflow
  mutate(
    total_effect = abs(sum_counterfactual_hist_nat - sum_CO2_obs),
    rainfall_effect = abs(sum_counterfactual_hist_nat -  sum_counterfactual_obs),
    partitioning_effect = abs(sum_counterfactual_obs - sum_CO2_obs) 
  ) |> 
  # relative impact to total - need if else when CO2 adds streamflow
  mutate(
    relative_rainfall_effect = if_else(total_effect > rainfall_effect, rainfall_effect / total_effect, total_effect / rainfall_effect) ,
    relative_partitioning_effect = if_else(total_effect > partitioning_effect, partitioning_effect / total_effect, total_effect / partitioning_effect) 
  ) |>  # join lat lon and state data
  left_join(
    lat_lon_data,
    by = join_by(gauge)
  )


## Stick results in a map (decade comparison side-by-side) =====================

# Map plotting function --------------------------------------------------------
map_plot <- function(plotting_variable, data, scale_limits, colour_palette, legend_title) {

  
  data <- data |>
    rename(
      plotting_variable = {{ plotting_variable }}
    )
  
  
  ## Make map template using ozmaps ============================================
  aus_map <- generate_aus_map_sf()
  
  ## Get inset data ============================================================
  ### Filter data by state #####################################################
  QLD_data <- data |>
    filter(state == "QLD")
  
  NSW_data <- data |>
    filter(state == "NSW")
  
  VIC_data <- data |>
    filter(state == "VIC")
  
  WA_data <- data |>
    filter(state == "WA")
  
  TAS_data <- data |>
    filter(state == "TAS")
  
  
  
  
  ## Generate inset plots ======================================================
  inset_plot_QLD <- aus_map |>
    filter(state == "QLD") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = QLD_data,
      aes(x = lon, y = lat, fill = plotting_variable),
      show.legend = FALSE,
      size = 2.5,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    theme_void()
  
  inset_plot_NSW <- aus_map |>
    filter(state == "NSW") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = NSW_data,
      aes(x = lon, y = lat, fill = plotting_variable),
      show.legend = FALSE,
      size = 2.5,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    theme_void()
  
  
  inset_plot_VIC <- aus_map |>
    filter(state == "VIC") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = VIC_data,
      aes(x = lon, y = lat, fill = plotting_variable),
      show.legend = FALSE,
      size = 2.5,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    # https://stackoverflow.com/questions/65947347/r-how-to-manually-set-binned-colour-scale-in-ggplot
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    theme_void()
  
  inset_plot_WA <- aus_map |>
    filter(state == "WA") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = WA_data,
      aes(x = lon, y = lat, fill = plotting_variable),
      show.legend = FALSE,
      size = 2.5,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    theme_void()
  
  
  
  inset_plot_TAS <- aus_map |>
    filter(state == "TAS") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = TAS_data,
      aes(x = lon, y = lat, fill = plotting_variable),
      show.legend = FALSE,
      size = 2.5,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    theme_void()
  
  
  ## The big map ===============================================================
  aus_map |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = data,
      aes(x = lon, y = lat, fill = plotting_variable),
      size = 2.5,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    theme_bw() +
    # expand map
    coord_sf(xlim = c(95, 176), ylim = c(-60, 0)) +
    # magnify WA
    geom_magnify(
      from = c(114, 118, -35.5, -30),
      to = c(93, 112, -36, -10),
      shadow = FALSE,
      expand = 0,
      plot = inset_plot_WA,
      proj = "single"
    ) +
    # magnify VIC
    geom_magnify(
      # aes(from = state == "VIC"), # use aes rather than manually selecting area
      from = c(141, 149.5, -39, -34),
      to = c(95, 136, -38, -60),
      shadow = FALSE,
      plot = inset_plot_VIC,
      proj = "single"
    ) +
    # magnify QLD
    geom_magnify(
      from = c(145, 155, -29.2, -15),
      to = c(157, 178, -29.5, 1.5),
      shadow = FALSE,
      expand = 0,
      plot = inset_plot_QLD,
      proj = "single"
    ) +
    # magnify NSW
    geom_magnify(
      from = c(146.5, 154, -38, -28.1),
      to = c(157, 178, -61, -30.5),
      shadow = FALSE,
      expand = 0,
      plot = inset_plot_NSW,
      proj = "single"
    ) +
    # magnify TAS
    geom_magnify(
      from = c(144, 149, -40, -44),
      to = c(140, 155, -45, -61),
      shadow = FALSE,
      expand = 0,
      plot = inset_plot_TAS,
      proj = "single"
    ) +
    labs(
      x = NULL, # "Latitude",
      y = NULL, # "Longitude",
      fill = legend_title
    ) +
    theme(
      legend.title = element_text(hjust = 0.5),
      legend.title.position = "top",
      legend.background = element_rect(colour = "black"),
      axis.text = element_blank(),
      legend.position = "inside",
      legend.position.inside = c(0.346, 0.9), # constants used to move the legend in the right place
      legend.box = "horizontal", # side-by-side legends
      #panel.border = element_blank(),
      panel.grid = element_blank(),
      axis.ticks = element_blank(),
      legend.margin = margin(t = 5, b = 5, r = 20, l = 20, unit = "pt") # add extra padding around legend box to avoid -1.6 intersecting with line
    ) +
    guides(
      fill = guide_colourbar(
        direction = "horizontal",
        barwidth = unit(12, "cm")
      )
    )
}



## Plot 1990-1999 ==============================================================
figure_label_1990 <- tribble(
  ~lon, ~lat, ~label_name,
  95, 0, "A"
)

decade_label_1990 <- tribble(
  ~lon, ~lat, ~label_name,
  105, 0, "1990-1999"
)

rainfall_impact_1990 <- map_plot(
  plotting_variable = relative_rainfall_effect,
  data =  decade_specific_decomposed_impacts |> filter(decade == 1),
  scale_limits = c(0, 1), 
  colour_palette = "RdYlBu",
  legend_title = "Relative effect of climate change on streamflow only rainfall (rainfall_effect / total_effect)" 
) +
  geom_text(
    data = figure_label_1990,
    aes(x = lon, y = lat, label = label_name),
    fontface = "bold",
    size = 10,
    size.unit = "pt"
  ) +
  geom_text(
    data = decade_label_1990,
    aes(x = lon, y = lat, label = label_name),
    size = 10,
    size.unit = "pt"
  )


## Plot 2012-2021 ==============================================================
figure_label_2012 <- tribble(
  ~lon, ~lat, ~label_name,
  95, 0, "B"
)

decade_label_2012 <- tribble(
  ~lon, ~lat, ~label_name,
  105, 0, "2012-2021"
)

rainfall_impact_2012 <- map_plot(
  plotting_variable = relative_rainfall_effect,
  data =  decade_specific_decomposed_impacts |> filter(decade == 2),
  scale_limits = c(0, 1), 
  colour_palette = "RdYlBu",
  legend_title = "Relative effect of climate change on streamflow only rainfall (rainfall_effect / total_effect)"
) +
  geom_text(
    data = figure_label_2012,
    aes(x = lon, y = lat, label = label_name),
    fontface = "bold",
    size = 10,
    size.unit = "pt"
  ) +
  geom_text(
    data = decade_label_2012,
    aes(x = lon, y = lat, label = label_name),
    size = 10,
    size.unit = "pt"
  )

## patchwork together and save =================================================
final_plot_rainfall_effect_plot <- (rainfall_impact_1990 | rainfall_impact_2012) +
  plot_layout(guides = "collect") &
  theme(legend.position = "bottom")

ggsave(
  file = "CO2_relative_impact_of_rainfall_on_streamflow.pdf",
  path = "./Figures",
  plot = final_plot_rainfall_effect_plot,
  device = "pdf",
  width = 297,
  height = 210,
  units = "mm"
)



# 
decomposing_impacts |> 
  filter(gauge == "405274") |> 
  ggplot(aes(x = `Counterfactual - Hist Nat Precipitation`, y = `CO2 Model - Observed Precipitation`)) +
  geom_point() +
  geom_abline(slope = 1) +
  theme_bw()
