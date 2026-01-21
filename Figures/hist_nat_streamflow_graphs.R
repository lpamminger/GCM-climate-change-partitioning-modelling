# Produce graphs using hist_nat_streamflow results -----------------------------


# Import libraries -------------------------------------------------------------
pacman::p_load(tidyverse, truncnorm, sloop, arrow, furrr, ozmaps, sf, ggmagnify, ggplot2, patchwork)
# scatterpie

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
# gauge_ToE_greater_than_2014 <- c("401210", "410156", "411003", "603007", "204036", "224213")

hist_nat_streamflow_data <- open_dataset(
  source = "./Results/hist_nat_streamflow_data.parquet"
) |>
  collect() #|>
# filter(!gauge %in% gauge_ToE_greater_than_2014)


## cmaes streamflow ============================================================
high_evidence_ratio_gauges <- hist_nat_streamflow_data |>
  pull(gauge) |>
  unique()

best_CO2_model_per_catchment_CMAES <- read_csv(
  "Previous/Results/best_CO2_non_CO2_per_catchment_CMAES.csv",
  show_col_types = FALSE
) |>
  filter(contains_CO2) |>
  # filter(gauge %in% high_evidence_ratio_gauges) |>
  select(gauge, streamflow_model)

cmaes_streamflow <- read_csv(
  "./Previous/Results/cmaes_streamflow_results.csv",
  show_col_types = FALSE
) |>
  semi_join(
    best_CO2_model_per_catchment_CMAES,
    by = join_by(gauge, streamflow_model)
  ) #|>
# filter(!gauge %in% gauge_ToE_greater_than_2014)


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

### split catchments ###########################################################
handpicked_catchments <- c("606195", "227210", "405240", "319204", "138004B")

supp_catchments <- hist_nat_plotting_data |> 
  filter(!gauge %in% handpicked_catchments) |> 
  pull(gauge) |> 
  unique()

chunk <- 7
n <- length(supp_catchments)
split_group <- rep(rep(1:ceiling(n/chunk), each = chunk))[1:n]
split_tibble <- tibble(
  "gauge" = supp_catchments,
  "split" = split_group
)

supp_data <- all_plotting_data |> 
  right_join(
    split_tibble,
    by = join_by(gauge)
  )

# converting table to list by groups https://stackoverflow.com/questions/7060272/split-up-a-dataframe-by-number-of-rows
chunked_supp_data <- supp_data |> 
  group_by(split) |> 
  group_map(~ .x)


### Make facet labels for plots ################################################
make_facet_labels <- function(data, facet_column, x_axis_column, y_axis_column, label_type = LETTERS, hjust = 0, vjust = 0) {
  # The embrace operator does not work correctly in summarise i.e., max({{ y_axis_column }})
  # Link: https://forum.posit.co/t/embrace-operator-for-tidy-selection-vs-data-masking/173084
  # Possible cause: {{ y_axis_column }} isn't unquoting when it's doing the mutate
  # Work around using rlang::ensym

  col <- rlang::ensym(y_axis_column)

  data |>
    summarise(
      ylab = max(!!col),
      min_ylab = min(!!col),
      .by = {{ facet_column }}
    ) |>
    # Add xlab - constant x-axis
    add_column(
      xlab = data |> pull(x_axis_column) |> min(),
      .before = 2
    ) |> # add row numbers to tibble
    mutate(
      row_number = row_number(),
      .before = 1
    ) |> # add label type based on row number
    mutate(
      label_name = label_type[row_number]
    ) |>
    # set zero values to -1 the adjustment works
    mutate(
      range_ylab = abs(ylab - min_ylab),
      ylab = case_when(
        (range_ylab > 100) & near(ylab, 0, 10) ~ -10,
        (range_ylab < 100) & near(ylab, 0, 10) ~ -1,
        .default = ylab
      ) 
    ) |>
    # apply hjust and vjust
    mutate(
      xlab = if_else(xlab > 0, xlab + (xlab * hjust), xlab + (xlab * -hjust)),
      ylab = if_else(ylab > 0, ylab + (ylab * vjust), ylab + (ylab * -vjust))
    )
}


#### Plot function #############################################################

timeseries_plot <- function(data, envelope_data) {
  
  # Pull gauges for envelope data
  gauges <- data |> pull(gauge) |> unique()
  
  # create facet_labels tibble
  facet_labels <- make_facet_labels(
    data = data,
    facet_column = "gauge",
    x_axis_column = "year",
    y_axis_column = "median_GCM_realspace_streamflow",
    label_type = LETTERS[1:length(gauges)],
    hjust = 0.0005,
    vjust = -0.05
  )
  
  data |>
    ggplot(aes(x = year, y = median_GCM_realspace_streamflow, shape = type, colour = type)) +
    geom_ribbon(
      aes(x = year, ymin = min_GCM_realspace_streamflow, ymax = max_GCM_realspace_streamflow),
      alpha = 0.2,
      inherit.aes = FALSE,
      data = envelope_data |> filter(gauge %in% gauges),
      fill = "#7570b3"
    ) +
    geom_text(
      mapping = aes(x = xlab, y = ylab, label = label_name),
      data = facet_labels,
      inherit.aes = FALSE,
      fontface = "bold",
      size = 10,
      size.unit = "pt"
    ) +
    geom_line() +
    geom_point() +
    theme_bw() +
    scale_shape_manual(
      labels = c(
        "With the Impact of Climate Change",
        "Only Climate Change Rainfall Shifts",
        "Without the Impact of Climate Change"
      ),
      values = c(15, 16, 17),
      drop = FALSE
    ) +
    scale_colour_brewer(
      palette = "Dark2",
      labels = c(
        "With the Impact of Climate Change",
        "Only Climate Change Rainfall Shifts",
        "Without the Impact of Climate Change"
      )
    ) +
    labs(
      x = "Time (Year)",
      y = "Streamflow (mm)",
      colour = NULL,
      shape = NULL
    ) +
    scale_x_continuous(expand = c(0.01, 0.01)) +
    theme(
      legend.position = "bottom",
      text = element_text(family = "sans", size = 9), # default fonts are serif, sans and mono, text size is in pt
      strip.background = element_blank(), # remove facet_strip gauge numbers
      strip.text = element_blank() # remove facet_strip gauge numbers
    ) +
    facet_wrap(~gauge, ncol = 1, scale = "free_y")
}



### Main figure plot ###########################################################
handpicked_plotting_data <- all_plotting_data |>
  filter(gauge %in% handpicked_catchments)


main_timeseries_plot <- timeseries_plot(
  data = handpicked_plotting_data, 
  envelope_data = hist_nat_plotting_data
)

ggsave(
  file = "handpicked_timeseries.pdf",
  path = "./Figures/Main",
  plot = main_timeseries_plot,
  device = "pdf",
  width = 183,
  height = 197,
  units = "mm"
)

### Supplemental plots #########################################################
supp_timeseries_plots <- map(
  .x = chunked_supp_data,
  .f = timeseries_plot,
  envelope_data = hist_nat_plotting_data
)

save_supp_plots <- function(ggplot_object, identifier, filename) {
  ggsave(
    file = paste0(filename, "_", identifier, ".pdf"),
    path = "./Figures/Supplementary",
    plot = ggplot_object,
    device = "pdf",
    width = 183,
    height = 197,
    units = "mm"
  )
}


create_caption <- function(identifier, chunked_supp_data) {
  
  gauge <- chunked_supp_data[[identifier]] |> pull(gauge) |> unique() |> sort()
  abc <- LETTERS[1:length(gauge)]
  gauge_abc <- paste0(gauge, " (", abc, ")")
  # concatenate everything but last value
  start_gauge_abc <- paste0(gauge_abc[1:(length(gauge_abc) - 2)], ", ", collapse = "")
  end_gauge_abc <- paste0(gauge_abc[(length(gauge_abc) - 1)], " and ", gauge_abc[length(gauge_abc)], ".")
  gauge_text <- paste(c(start_gauge_abc, end_gauge_abc), collapse = "")
  
  cat("\\begin{figure}") 
  cat("\n")
  cat("\t\\centering")
  cat("\n")
  cat(paste0("\t\\includegraphics[width=\\textwidth]{Figures/Chapter 5/chapt_5_supp_streamflow_timeseries_", identifier, ".pdf}"))
  cat("\t\n")
  # The line below must change
  cat(paste0("\t\\caption{\\textbf{Streamflow time series decomposing the impact of climate change into rainfall and rainfall-partitioning for gauges ", gauge_text, "} Same as Figure~\\ref{fig:chapt_5_streamflow_timeseries}.}"))
  cat("\n")
  # The line below must change
  cat(paste0("\t\\label{fig:chapt_5_supp_streamflow_timeseries_", identifier, "}")) 
  cat("\n")
  cat("\\end{figure}")
  cat("\n")
  cat("\n")
  
}


iwalk(
  .x = supp_timeseries_plots,
  .f = save_supp_plots,
  filename = "chapt_5_supp_streamflow_timeseries"
)


sink(file = "Figures/Supplementary/streamflow_time_captions_supp.txt") # filename must change
walk(
  .x = seq(from = 1, to = length(chunked_supp_data)),
  .f = create_caption,
  chunked_supp_data = chunked_supp_data
)
sink()







# Decompose of rainfall and CO2 partitioning on streamflow ---------------------

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
# mutate(
#  total_effect = `Counterfactual - Hist Nat Precipitation` - `CO2 Model - Observed Precipitation`,
#  rainfall_effect = `Counterfactual - Hist Nat Precipitation` - `Counterfactual - Observed Precipitation`,
#  partitioning_effect = `Counterfactual - Observed Precipitation` - `CO2 Model - Observed Precipitation`
# ) |>
# find relative impact
# mutate(
#  relative_rainfall_effect = rainfall_effect / total_effect,
#  relative_partitioning_effect = partitioning_effect / total_effect
# )


## Aggregate impacts on two specific decades ===================================
### Do the calcuation on the total rather than year specific average
decade_1 <- seq(from = 1990, to = 1999)
decade_2 <- seq(from = 2005, to = 2014) # these must be altered

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
    rainfall_effect = abs(sum_counterfactual_hist_nat - sum_counterfactual_obs),
    partitioning_effect = abs(sum_counterfactual_obs - sum_CO2_obs)
  ) |>
  # relative impact to total - need if else when CO2 adds streamflow
  mutate(
    relative_rainfall_effect = if_else(total_effect > rainfall_effect, rainfall_effect / total_effect, total_effect / rainfall_effect),
    relative_partitioning_effect = if_else(total_effect > partitioning_effect, partitioning_effect / total_effect, total_effect / partitioning_effect)
  ) |> # join lat lon and state data
  left_join(
    lat_lon_data,
    by = join_by(gauge)
  )


## Adding uncertainty ==========================================================
# 1. This is all the GCM streamflow results - This is needed for total and rainfall calculations
GCM_streamflow_data <- hist_nat_streamflow_data |>
  filter(GCM != "observed") |>
  # Ensemble median per GCM
  summarise(
    median_ensemble_realspace_streamflow = median(realspace_streamflow),
    .by = c(year, GCM, gauge)
  ) |>
  add_column(
    type = "Counterfactual - Hist Nat Precipitation"
  ) |> # 2. Aggregate into decade form
  filter(year %in% c(decade_1, decade_2)) |>
  # add decade column
  mutate(
    decade = case_when(
      year %in% decade_1 ~ 1,
      year %in% decade_2 ~ 2,
      .default = NA
    )
  ) |>
  summarise(
    sum_counterfactual_hist_nat = sum(median_ensemble_realspace_streamflow),
    .by = c(decade, gauge, GCM)
  )

# 3. Total effect = `Counterfactual - Hist Nat Precipitation` - `CO2 Model - Observed Precipitation`
## join decade specific decomposed impacts
uncertainty_decade_specific_decomposed_impacts <- decade_specific_decomposed_impacts |>
  select(gauge, decade, sum_counterfactual_obs, sum_CO2_obs) |>
  right_join(
    GCM_streamflow_data,
    by = join_by(decade, gauge)
  ) |>
  mutate(
    total_effect = abs(sum_counterfactual_hist_nat - sum_CO2_obs),
    partitioning_effect = abs(sum_counterfactual_obs - sum_CO2_obs)
  ) |> # get relative effect for partitioning only to avoid double up of uncertaities
  mutate(
    relative_partitioning_effect = if_else(total_effect > partitioning_effect, partitioning_effect / total_effect, total_effect / partitioning_effect),
    # assume relative_rainfall_effect is complementary
    relative_rainfall_effect = 1 - relative_partitioning_effect
  ) |>
  # account for total effect percentage change uncertainty
  mutate(
    total_effect_CC_percent = if_else(
      total_effect < partitioning_effect,
      total_effect / sum_counterfactual_hist_nat,
      -total_effect / sum_counterfactual_hist_nat
    ) * 100
  ) |>
  # get IQR as a measure of uncertainty
  summarise(
    range_relative_rainfall_effect = IQR(relative_partitioning_effect), # max(range(relative_rainfall_effect)) - min(range(relative_rainfall_effect)),
    range_total_CC_percentage_effect = IQR(total_effect_CC_percent),
    .by = c(gauge, decade)
  ) |> # 4. join uncertainty values to existing values
  right_join(
    decade_specific_decomposed_impacts,
    by = join_by(gauge, decade)
  ) |> # arrange by uncertainty for best plotting
  arrange(desc(range_relative_rainfall_effect))

## Stick results in a map (decade comparison side-by-side) =====================

# Map plotting function --------------------------------------------------------
map_plot <- function(plotting_variable, size_variable, data, scale_limits, uncertainty_limits, uncertainty_breaks, colour_palette, legend_title) {
  data <- data |>
    rename(
      plotting_variable = {{ plotting_variable }},
      size_variable = {{ size_variable }}
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
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()

  inset_plot_NSW <- aus_map |>
    filter(state == "NSW") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = NSW_data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()


  inset_plot_VIC <- aus_map |>
    filter(state == "VIC") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = VIC_data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()

  inset_plot_WA <- aus_map |>
    filter(state == "WA") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = WA_data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()


  inset_plot_TAS <- aus_map |>
    filter(state == "TAS") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = TAS_data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()


  ## The big map ===============================================================
  aus_map |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = scale_limits,
      direction = 2
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
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
      fill = legend_title,
      size = "IQR from GCM Uncertainty"
    ) +
    theme(
      legend.title = element_text(hjust = 0.5),
      legend.title.position = "top",
      legend.background = element_rect(colour = "black"),
      axis.text = element_blank(),
      legend.position = "inside",
      legend.position.inside = c(0.346, 0.9), # constants used to move the legend in the right place
      legend.box = "horizontal", # side-by-side legends
      # panel.border = element_blank(),
      panel.grid = element_blank(),
      axis.ticks = element_blank(),
      # legend.key.width = unit(1, "null"), this looks nice but I can't get it to work https://tidyverse.org/blog/2024/02/ggplot2-3-5-0-legends/
      legend.margin = margin(t = 5, b = 5, r = 20, l = 20, unit = "pt") # add extra padding around legend box to avoid -1.6 intersecting with line
    ) +
    guides(
      fill = guide_colourbar(
        direction = "horizontal",
        barwidth = unit(12, "cm")
      ),
      size = guide_bins(
        show.limits = TRUE,
        direction = "horizontal",
        keywidth = 2
      )
    )
}


## Calculate limits and breaks for uncertainty =================================
uncertainty_decade_specific_decomposed_impacts |>
  pull(range_relative_rainfall_effect) |>
  range()


# hard-code limits and range
uncertainty_dot_limits <- c(0, 0.44)
uncertainty_dot_breaks <- c(0.05, 0.10, 0.15, 0.20, 0.25, 0.3, 0.35, 0.4)

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
  size_variable = range_relative_rainfall_effect,
  data = uncertainty_decade_specific_decomposed_impacts |> filter(decade == 1),
  scale_limits = c(0, 1),
  uncertainty_limits = uncertainty_dot_limits,
  uncertainty_breaks = uncertainty_dot_breaks,
  colour_palette = "RdYlBu",
  legend_title = "Relative Impact of Rainfall on Streamflow (rainfall effect / total effect)"
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


## Plot 2005-2014 ==============================================================
figure_label_2012 <- tribble(
  ~lon, ~lat, ~label_name,
  95, 0, "B"
)

decade_label_2012 <- tribble(
  ~lon, ~lat, ~label_name,
  105, 0, "2005-2014"
)

rainfall_impact_2012 <- map_plot(
  plotting_variable = relative_rainfall_effect,
  size_variable = range_relative_rainfall_effect,
  data = uncertainty_decade_specific_decomposed_impacts |> filter(decade == 2),
  scale_limits = c(0, 1),
  uncertainty_limits = uncertainty_dot_limits,
  uncertainty_breaks = uncertainty_dot_breaks,
  colour_palette = "RdYlBu",
  legend_title = "Relative Impact of Rainfall on Streamflow (rainfall effect / total effect)"
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
  file = "map_CO2_relative_impact_of_rainfall_on_streamflow.pdf",
  path = "./Figures/Main",
  plot = final_plot_rainfall_effect_plot,
  device = "pdf",
  width = 297,
  height = 210,
  units = "mm"
)


## for results get count of relative_rainfall_effect ===========================
### Compare catchment with partitioning having greater effect than rainfall between decades
uncertainty_decade_specific_decomposed_impacts |>
  filter(decade == 2) |> # change to 2
  filter(relative_partitioning_effect >= 0.5) |>
  pull(relative_partitioning_effect) |>
  length()

### Compare Australia wide average fraction between decades
uncertainty_decade_specific_decomposed_impacts |>
  filter(decade == 1) |>
  pull(relative_partitioning_effect) |>
  mean()

### Find the contributions in 2014
uncertainty_decade_specific_decomposed_impacts |> 
  filter(decade == 2) |> 
  summarise(
    sum_total_effect = sum(total_effect),
    sum_rainfall_effect = sum(rainfall_effect),
    sum_partitioning_effect = sum(partitioning_effect)
  ) |> 
  mutate(
    percent_partitioning_effect = (sum_partitioning_effect / sum_total_effect) * 100,
    percent_rainfall_effect = (sum_rainfall_effect / sum_total_effect) * 100
  )
  
  
# Plot total, rainfall and partitioning effect timeseries ----------------------
## Total effect = `Counterfactual - Hist Nat Precipitation` - `CO2 Model - Observed Precipitation`
## Rainfall effect = `Counterfactual - Hist Nat Precipitation` - `Counterfactual - Observed Precipitation`
## Partitioning effect = `Counterfactual - Observed Precipitation` - `CO2 Model - Observed Precipitation`


decomposed_timeseries_data <- decomposing_impacts |>
  # decompose using formula above
  mutate(
    total_effect = `Counterfactual - Hist Nat Precipitation` - `CO2 Model - Observed Precipitation`,
    rainfall_effect = `Counterfactual - Hist Nat Precipitation` - `Counterfactual - Observed Precipitation`,
    partitioning_effect = `Counterfactual - Observed Precipitation` - `CO2 Model - Observed Precipitation`
  ) |>
  pivot_longer(
    cols = ends_with("effect"),
    names_to = "effect",
    values_to = "streamflow"
  ) |>
  mutate(
    streamflow = -1 * streamflow # multiple by negative 1 to get loss and gain correct
  ) |> 
  mutate(
    effect = case_when(
      effect == "partitioning_effect" ~ "Rainfall-Partitioning Effect",
      effect == "rainfall_effect" ~ "Rainfall Effect",
      effect == "total_effect" ~ "Total Effect"
    )
  )


## plot and save =============================================================== 

### split catchments ###########################################################
supp_difference_data <- decomposed_timeseries_data |> 
  right_join(
    split_tibble,
    by = join_by(gauge)
  )

# converting table to list by groups https://stackoverflow.com/questions/7060272/split-up-a-dataframe-by-number-of-rows
chunked_difference_supp_data <- supp_difference_data |> 
  group_by(split) |> 
  group_map(~ .x)



#### Plot function #############################################################

timeseries_difference_plot <- function(data) {
  
  # Pull gauges for envelope data
  gauges <- data |> pull(gauge) |> unique()
  
  # create facet_labels tibble
  facet_labels <- make_facet_labels(
    data = data,
    facet_column = "gauge",
    x_axis_column = "year",
    y_axis_column = "streamflow",
    label_type = LETTERS[1:length(gauges)],
    hjust = 0.0005,
    vjust = -0.2
  )
  
  data |> 
  ggplot(aes(x = year, y = streamflow, shape = effect, colour = effect)) +
    geom_line() +
    geom_point() +
    # geom_hline(yintercept = 0, linetype = "dashed") +
    geom_text(
      mapping = aes(x = xlab, y = ylab, label = label_name),
      data = facet_labels,
      inherit.aes = FALSE,
      fontface = "bold",
      size = 10,
      size.unit = "pt"
    ) +
    facet_wrap(~gauge, scales = "free_y") +
    scale_colour_brewer(palette = "Dark2") +
    theme_bw() +
    labs(
      y = "Climate Change Induced Shift in Streamflow (mm)",
      x = "Year",
      colour = NULL,
      shape = NULL
    ) +
    scale_x_continuous(expand = c(0.01, 0.01)) +
    theme(
      legend.position = "bottom",
      text = element_text(family = "sans", size = 9), # default fonts are serif, sans and mono, text size is in pt
      strip.background = element_blank(), # remove facet_strip gauge numbers
      strip.text = element_blank() # remove facet_strip gauge numbers
    ) +
    facet_wrap(~gauge, ncol = 1, scale = "free_y")
}



### Main figure plot ###########################################################
main_decomposed_timeseries_plot <- decomposed_timeseries_data |>
  filter(gauge %in% handpicked_catchments) |> 
  timeseries_difference_plot()


ggsave(
  file = "handpicked_climate_change_effect_timeseries.pdf",
  path = "./Figures/Main",
  plot = main_decomposed_timeseries_plot,
  device = "pdf",
  width = 183,
  height = 197,
  units = "mm"
)


### Supplemental plots #########################################################
supp_decomposed_timeseries_plots <- map(
  .x = chunked_difference_supp_data,
  .f = timeseries_difference_plot
)


decomposed_create_caption <- function(identifier, chunked_supp_data) {
  
  gauge <- chunked_supp_data[[identifier]] |> pull(gauge) |> unique() |> sort()
  abc <- LETTERS[1:length(gauge)]
  gauge_abc <- paste0(gauge, " (", abc, ")")
  # concatenate everything but last value
  start_gauge_abc <- paste0(gauge_abc[1:(length(gauge_abc) - 2)], ", ", collapse = "")
  end_gauge_abc <- paste0(gauge_abc[(length(gauge_abc) - 1)], " and ", gauge_abc[length(gauge_abc)], ".")
  gauge_text <- paste(c(start_gauge_abc, end_gauge_abc), collapse = "")
  
  cat("\\begin{figure}") 
  cat("\n")
  cat("\t\\centering")
  cat("\n")
  cat(paste0("\t\\includegraphics[width=\\textwidth]{Figures/Chapter 5/chapt_5_supp_climate_change_effect_timeseries_", identifier, ".pdf}"))
  cat("\t\n")
  # The line below must change
  cat(paste0("\t\\caption{\\textbf{Change in streamflow over time due to climate change induced changes in rainfall and rainfall-partitioning for gauges ", gauge_text, "} Same as Figure~\\ref{fig:chapt_5_cc_effect_timeseries}.}"))
  cat("\n")
  # The line below must change
  cat(paste0("\t\\label{fig:chapt_5_supp_climate_change_effect_timeseries_", identifier, "}")) 
  cat("\n")
  cat("\\end{figure}")
  cat("\n")
  cat("\n")
  
}


iwalk(
  .x = supp_decomposed_timeseries_plots,
  .f = save_supp_plots,
  filename = "chapt_5_supp_climate_change_effect_timeseries"
)


sink(file = "Figures/Supplementary/decomposed_streamflow_time_captions_supp.txt") # filename must change
walk(
  .x = seq(from = 1, to = length(chunked_difference_supp_data)),
  .f = decomposed_create_caption,
  chunked_supp_data = chunked_difference_supp_data
)
sink()





### change plotting function
### change caption function

###
supp_data <- all_plotting_data |> 
  right_join(
    split_tibble,
    by = join_by(gauge)
  )

# converting table to list by groups https://stackoverflow.com/questions/7060272/split-up-a-dataframe-by-number-of-rows
chunked_supp_data <- supp_data |> 
  group_by(split) |> 
  group_map(~ .x)




## giant timeseries plot =======================================================
facet_label_effect <- make_facet_labels(
  data = decomposed_timeseries_data |> filter(gauge %in% handpicked_catchments),
  facet_column = "gauge",
  x_axis_column = "year",
  y_axis_column = "streamflow",
  label_type = LETTERS,
  hjust = 0.0005,
  vjust = -0.2
)

plot_decomposed_timeseries <- decomposed_timeseries_data |>
  filter(gauge %in% handpicked_catchments) |>
  ggplot(aes(x = year, y = streamflow, shape = effect, colour = effect)) +
  geom_line() +
  geom_point() +
  # geom_hline(yintercept = 0, linetype = "dashed") +
  geom_text(
    mapping = aes(x = xlab, y = ylab, label = label_name),
    data = facet_label_effect,
    inherit.aes = FALSE,
    fontface = "bold",
    size = 10,
    size.unit = "pt"
  ) +
  facet_wrap(~gauge, scales = "free_y") +
  scale_colour_brewer(palette = "Dark2") +
  theme_bw() +
  labs(
    y = "Climate Change Induced Shift in Streamflow (mm)",
    x = "Year",
    colour = NULL,
    shape = NULL
  ) +
  scale_x_continuous(expand = c(0.01, 0.01)) +
  theme(
    legend.position = "bottom",
    text = element_text(family = "sans", size = 9), # default fonts are serif, sans and mono, text size is in pt
    strip.background = element_blank(), # remove facet_strip gauge numbers
    strip.text = element_blank() # remove facet_strip gauge numbers
  ) +
  facet_wrap(~gauge, ncol = 1, scale = "free_y")


plot_decomposed_timeseries














# Map of total impact ----------------------------------------------------------
## Total effect = `Counterfactual - Hist Nat Precipitation` - `CO2 Model - Observed Precipitation`
## Rainfall effect = `Counterfactual - Hist Nat Precipitation` - `Counterfactual - Observed Precipitation`
## Partitioning effect = `Counterfactual - Observed Precipitation` - `CO2 Model - Observed Precipitation`


## Total effect - for a given decade the total effect of climate change
##                has reduced (added for 2 catchments) streamflow by X mm
##                A percentage change seems better.
##                A percentage change in streamflow from climate change
##                streamflow_with_CC - streamflow_with_CC / streamflow_without_CC = - total_effect / sum_counterfactual_hist_nat (except 2)


# Use relative rainfall effect column (total_effect_CC_percent * relative_rainfall) - see if it gives me the same results
# it does
total_effect_data <- uncertainty_decade_specific_decomposed_impacts |>
  mutate(
    total_effect_CC_percent = if_else(
      total_effect < partitioning_effect,
      total_effect / sum_counterfactual_hist_nat,
      -total_effect / sum_counterfactual_hist_nat
    ) #* 100 - label_percent() does this
  ) |>
  # use total effect and rainfall effect to estimate components
  mutate(
    rainfall_effect_CC_percent = relative_rainfall_effect * total_effect_CC_percent,
    partitioning_effect_CC_percent = (1 - relative_rainfall_effect) * total_effect_CC_percent
  )


## Statistics for the results ==================================================
total_effect_data |>
  mutate(total_effect_CC_percent = total_effect_CC_percent * 100) |>
  summarise(
    mean = mean(total_effect_CC_percent),
    sd = sd(total_effect_CC_percent),
    mean_uncertainty = mean(range_total_CC_percentage_effect),
    .by = decade
  )


total_effect_data |>
  mutate(total_effect_CC_percent = total_effect_CC_percent * 100) |>
  summarise(
    mean = mean(total_effect_CC_percent),
    sd = sd(total_effect_CC_percent),
    mean_uncertainty = mean(range_total_CC_percentage_effect),
    n = n(),
    .by = c(decade, state)
  ) |>
  arrange(state, decade)


## Scale legends correctly =====================================================

### Size variable ##############################################################
total_effect_data |>
  pull(range_total_CC_percentage_effect) |>
  range()
uncertainty_dot_limits <- c(1.6, 37) # HARD CODED
uncertainty_dot_breaks <- seq(from = 1.6, to = 37, length.out = 7)


### Fill variable ##############################################################
total_effect_data |>
  pull(total_effect_CC_percent) |>
  range()
main_variable_limits <- c(-0.84, 0.11) # HARD CODED

main_variable_breaks <- sort(c(seq(from = -0.84, to = 0, length.out = 5), 0.11))

rescale_colourbar <- main_variable_breaks |>
  scales::rescale(to = c(0, 1))

# Need to adjust rescale_colourbar. The zero values is too blue it should be on the transition
# edit manually - make the zero on the edge of yellow and blue. Make numbers bigger to do this
rescale_colourbar <- c(0.0, 0.45, 0.7, 0.9, 0.99, 1.0)


test_map_plot <- function(plotting_variable, size_variable, data, main_variable_limits, main_variable_breaks, rescale_colourbar, uncertainty_limits, uncertainty_breaks, colour_palette, legend_title) {
  data <- data |>
    rename(
      plotting_variable = {{ plotting_variable }},
      size_variable = {{ size_variable }}
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
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = main_variable_limits,
      breaks = main_variable_breaks,
      values = rescale_colourbar,
      labels = scales::label_percent(),
      direction = 1
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()


  inset_plot_NSW <- aus_map |>
    filter(state == "NSW") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = NSW_data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = main_variable_limits,
      breaks = main_variable_breaks,
      values = rescale_colourbar,
      labels = scales::label_percent(),
      direction = 1
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()


  inset_plot_VIC <- aus_map |>
    filter(state == "VIC") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = VIC_data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = main_variable_limits,
      breaks = main_variable_breaks,
      values = rescale_colourbar,
      labels = scales::label_percent(),
      direction = 1
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()

  inset_plot_WA <- aus_map |>
    filter(state == "WA") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = WA_data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = main_variable_limits,
      breaks = main_variable_breaks,
      values = rescale_colourbar,
      labels = scales::label_percent(),
      direction = 1
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()


  inset_plot_TAS <- aus_map |>
    filter(state == "TAS") |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = TAS_data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      show.legend = FALSE,
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = main_variable_limits,
      breaks = main_variable_breaks,
      values = rescale_colourbar,
      labels = scales::label_percent(),
      direction = 1
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
    theme_void()


  ## The big map ===============================================================
  aus_map |>
    ggplot() +
    geom_sf() +
    geom_point(
      data = data,
      aes(x = lon, y = lat, fill = plotting_variable, size = size_variable),
      stroke = 0.1,
      colour = "black",
      shape = 21
    ) +
    scale_fill_distiller(
      palette = colour_palette,
      limits = main_variable_limits,
      breaks = main_variable_breaks,
      values = rescale_colourbar,
      labels = scales::label_percent(),
      direction = 1
    ) +
    scale_size_binned(
      limits = uncertainty_limits,
      breaks = uncertainty_breaks,
      range = c(1, 4)
    ) +
    guides(size = guide_bins(show.limits = TRUE)) +
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
      fill = legend_title,
      size = "IQR from GCM Uncertainty"
    ) +
    theme(
      legend.title = element_text(hjust = 0.5),
      legend.title.position = "top",
      legend.background = element_rect(colour = "black"),
      axis.text = element_blank(),
      legend.position = "inside",
      legend.position.inside = c(0.346, 0.9), # constants used to move the legend in the right place
      legend.box = "horizontal", # side-by-side legends
      # panel.border = element_blank(),
      panel.grid = element_blank(),
      axis.ticks = element_blank(),
      # legend.key.width = unit(1, "null"), this looks nice but I can't get it to work https://tidyverse.org/blog/2024/02/ggplot2-3-5-0-legends/
      legend.margin = margin(t = 5, b = 5, r = 20, l = 20, unit = "pt") # add extra padding around legend box to avoid -1.6 intersecting with line
    ) +
    guides(
      fill = guide_colourbar(
        direction = "horizontal",
        barwidth = unit(12, "cm")
      ),
      size = guide_bins(
        show.limits = TRUE,
        direction = "horizontal",
        keywidth = 2
      )
    )
}


## Total effect ================================================================
total_impact_1990 <- test_map_plot(
  plotting_variable = total_effect_CC_percent,
  size_variable = range_total_CC_percentage_effect,
  data = total_effect_data |> filter(decade == 1),
  main_variable_limits = main_variable_limits,
  main_variable_breaks = main_variable_breaks,
  rescale_colourbar = rescale_colourbar,
  uncertainty_limits = uncertainty_dot_limits,
  uncertainty_breaks = uncertainty_dot_breaks,
  colour_palette = "RdYlBu",
  legend_title = "Total Change in Streamflow Due to Climate Change"
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


total_impact_2012 <- test_map_plot(
  plotting_variable = total_effect_CC_percent,
  size_variable = range_total_CC_percentage_effect,
  data = total_effect_data |> filter(decade == 2),
  main_variable_limits = main_variable_limits,
  main_variable_breaks = main_variable_breaks,
  rescale_colourbar = rescale_colourbar,
  uncertainty_limits = uncertainty_dot_limits,
  uncertainty_breaks = uncertainty_dot_breaks,
  colour_palette = "RdYlBu",
  legend_title = "Total Change in Streamflow Due to Climate Change"
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


total_impact_of_CC_on_streamflow_plot <- (total_impact_1990 | total_impact_2012) +
  plot_layout(guides = "collect") &
  theme(legend.position = "bottom")


ggsave(
  file = "map_total_impact_of_CC_on_streamflow.pdf",
  path = "./Figures/Main",
  plot = total_impact_of_CC_on_streamflow_plot,
  device = "pdf",
  width = 297,
  height = 210,
  units = "mm"
)

