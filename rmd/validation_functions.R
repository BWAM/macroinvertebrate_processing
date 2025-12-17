
# Foreign Key Validation --------------------------------------------------

validate_fks <- function(df, table_name) {
  # Connect to the data warehouse.
  con <- DBI::dbConnect(
    drv = duckdb::duckdb(),
    dbdir = "L:/DOW/BWAM Share/data/data_warehouses/transfer_log/transfer_log.duckdb "
  )

  # Disconnect from the warehouse when the function is done.
  on.exit(DBI::dbDisconnect(con))

  # Path to the dictionary.
  dictionary_path <- "L:/DOW/BWAM Share/data/parquet/dictionaries/DICTIONARY.parquet"
  # Import the data dictionary to get the necessary info to perform FK comparisons.
  dictionary <- arrow::open_dataset(dictionary_path) |>
    # Keep only rows related to the target table.
    dplyr::filter(TABLE_NAME == table_name,
                  SURROGATE_KEY == FALSE) |>
    dplyr::select(COLUMN_NAME, PRIMARY_KEY, FOREIGN_KEY, DML_KEY, PARENT_TABLE) |>
    dplyr::collect()

  # Narrow down to just the rows associated with columns that have FKs.
  parent_tbls <- dictionary |>
    dplyr::filter(!PARENT_TABLE %in% "NONE") |>
    dplyr::select(PARENT_TABLE, COLUMN_NAME) |>
    dplyr::collect()

  # If there are no parent tables (i.e., no FKs), then end early with everything passing.
  if (nrow(parent_tbls) == 0) {
    final_df <- df |>
      dplyr::mutate(FK_VALIDATION_PASSED = TRUE)

    return(final_df)
  }

  # Directory to parent tables stored as parquet files.
  parent_dir <- "L:/DOW/BWAM Share/data/parquet/build_tables"

  # For each Parent table, check that there is a FK match.
  # This can be based on a single column (e.g., EVENT to SITE) or
  # multiple columns (e.g., RESULT to SAMPLE)
  # depending on the table being investigated.
  matched_df <- purrr::map(unique(parent_tbls$PARENT_TABLE), function(x) {
    # Get columns specific to the parent table in the current iteration.
    parent_cols <- parent_tbls |>
      dplyr::filter(PARENT_TABLE == x) |>
      dplyr::pull(COLUMN_NAME)

    # Path to the parent parquet file.
    parent_path <- file.path(
      parent_dir,
      paste0(x, ".parquet")
    )
    # Extract the FK columns/values that are currently in WQMAP.
    parent_wqma <- arrow::open_dataset(parent_path) |>
      dplyr::select(
        dplyr::all_of(parent_cols)
      ) |>
      dplyr::distinct() |>
      dplyr::collect() |>
      dplyr::mutate(SOURCE = "wqmap")

    staged_table <- DBI::dbGetQuery(con, glue::glue("SELECT EXISTS (SELECT DISTINCT TABLE_NAME
                                    FROM TRANSFER_LOG
                                    WHERE TABLE_NAME = '{x}') AS STAGED")) |>
      dplyr::pull(STAGED)

    if (staged_table == TRUE) {
      # Read in the most recent records from the TRANSFER_LOG related to the
      # target table.
      parent_staged <- dplyr::tbl(con, "TRANSFER_LOG") |>
        dplyr::filter(TABLE_NAME == x) |>
        # Keep only the most recent version of a record.
        get_current_version() |>
        dplyr::select(UID, DATA) |>
        dplyr::collect() |>
        dplyr::mutate(EXTRACT = (df_from_row_json(x = DATA))) |>
        tidyr::unnest_wider(EXTRACT) |>
        dplyr::select(
          dplyr::all_of(parent_cols)
        ) |>
        dplyr::distinct() |>
        dplyr::anti_join(
          y = parent_wqma,
          by = parent_cols
        ) |>
        dplyr::mutate(SOURCE = "staged")

      parent_df <- dplyr::bind_rows(
        parent_wqma,
        parent_staged
      ) |>
        # Create a new column, MATCHED, with a default value of TRUE.
        # When left joined below to the target df, only those rows that matched
        # will be kept, and therefore MATCHED should be set to TRUE.
        dplyr::mutate(MATCHED = TRUE)
    } else {
      parent_df <- parent_wqma |>
        # Create a new column, MATCHED, with a default value of TRUE.
        # When left joined below to the target df, only those rows that matched
        # will be kept, and therefore MATCHED should be set to TRUE.
        dplyr::mutate(MATCHED = TRUE)
    }


    matched <- dplyr::left_join(
      x = df,
      y = parent_df,
      by = parent_cols
    ) |>
      dplyr::mutate(
        # If MATCHED is NA, that means a match was not found during the left join.
        # These values should be updated to FALSE.
        MATCHED = tidyr::replace_na(MATCHED, FALSE)
      ) |>
      # Keep only the UID to link back to the original DF and the new matched column.
      dplyr::select(UID, MATCHED) |>
      # Rename the MATCHED column to include an indication of which table was
      # matched (e.g., "MATCHED_SITE" or "MATCHED_SAMPLE").
      dplyr::rename(
        !!glue::glue("MATCHED_{x}") := MATCHED
      )
  }) |>
    # Iteratively use full join on all matched outputs by
    purrr::reduce(dplyr::full_join, by = "UID") |>
    dplyr::left_join(
      x = df,
      y = _,
      by = "UID"
    )

  # Summarize if each row passed FK validation (TRUE) or if there were
  # 1 or more failures (FALSE).
  final_df <- matched_df |>
    # Needs to be done rowwise. Otherwise this looks at all values in the selected columns.
    dplyr::rowwise() |>
    dplyr::mutate(
      # Find all columns that start with "MATCHED" (starts_with)
      # Only evaluate those Matched columns (pick)
      # Check if all Matched columns are TRUE (all() == TRUE)
      # The == TRUE is not necessary, but I thought it was more clear what the
      # the code is intended to do when == TRUE is present.
      FK_VALIDATION_PASSED = all(dplyr::pick(dplyr::starts_with("MATCHED"))) == TRUE) |>
    # Remove the rowwise grouping.
    dplyr::ungroup()

  return(final_df)

}

# Table Validation --------------------------------------------------------


validate <- function(df, table_name, return_report = FALSE) {
  validation_df <- switch(
    table_name,
    "BASIN" = val_basin(df = df),
    "SITE" = val_site(df = df),
    "WATERBODY" = val_waterbody(df = df),
    "EVENT" = val_event(df = df),
    "PROJECT" = val_project(df = df),
    "SAMPLE_DELIVERY_GROUP" = val_sdg(df = df),
    "SAMPLE" = val_sample(df = df),
    "RESULT" = val_result(df = df),
    "RESULT_QUALIFIER" = val_result_qualifier(df = df),
    "PARAMETER" = val_parameter(df = df),
    "PARAMETER_NAME" = val_parameter_name(df = df),
    "QUALITY_CONTROL" = val_quality_control(df = df),
    rlang::abort(glue::glue_col("You supplied: '{yellow table}' which is not a recognized table in the validate function"))
  )


  if (return_report == FALSE) {
    validation_warning(multiagent = validation_df,
                     table_name = table_name)
  } else {
    return(validation_df)
  }


}

get_failing <- function(multiagent) {
  names(multiagent$agents) |>
    purrr::set_names() |>
    purrr::map(~pointblank::get_agent_report(multiagent$agent[[.x]],
                                             display_table = FALSE)) |>
    purrr::list_rbind(names_to = "check_type") |>
    dplyr::filter(f_pass < 1) |>
    dplyr::select(
      check_type,
      type,
      columns,
      values,
      units,
      n_pass,
      f_pass
    )
}

get_fail_summary <- function(fail_df) {
  fail_summary <- fail_df |>
    dplyr::count(check_type) |>
    dplyr::mutate(message = glue::glue("{n} {check_type} issues"))
}

validation_warning <- function(multiagent, table_name) {

  failing <- get_failing(multiagent = multiagent)
  n_fails <- nrow(failing)

  if (n_fails > 0) {
    validation_failures <<- failing
    fail_summary <- get_fail_summary(fail_df = failing)
    fail_error_message <- glue::glue_collapse(fail_summary$message, sep = ", ", last = ", and ") |> as.character()

    cli::cli_warn(
      c(
        "There are {n_fails} validation failure{?s} related to the {.strong {table_name}} table.",
        "i" = "the data frame {.strong validation_failures} has been added to your global environment for review.",
        "x" = "{.val {fail_error_message}}")
      )
  }
}


## BASIN -------------------------------------------------------------------


## WATERBODY ---------------------------------------------------------------

val_waterbody <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "WATERBODY",
    label = "Expected Columns Exist",
    actions = pointblank::action_levels(warn_at = 1)
  ) |>
    pointblank::col_exists(columns = c("WATERBODY_CODE")) |>
    pointblank::col_exists(columns = c("WATERBODY_NAME")) |>
    pointblank::col_exists(columns = c( "WATERBODY_TYPE")) |>
    pointblank::col_exists(columns = c("BEACH_PRESENT")) |>
    pointblank::col_exists(columns = c("FISHERIES_INDEX_NUMBER")) |>
    pointblank::col_exists(columns = c("PUBLIC_WATER_SUPPLY")) |>
    pointblank::col_exists(columns = c("USGS_POND_NUMBER")) |>
    pointblank::col_exists(columns = c("WATERBODY_ALTERNATE_NAME")) |>
    pointblank::col_is_character(
      columns = c(
        "BEACH_PRESENT",
        "WATERBODY_CODE",
        "WATERBODY_NAME",
        "WATERBODY_TYPE",
        "FISHERIES_INDEX_NUMBER",
        "PUBLIC_WATER_SUPPLY",
        "USGS_POND_NUMBER",
        "WATERBODY_ALTERNATE_NAME"
      ),
      step_id = "Checking character types"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(WATERBODY_CODE) <= 50,
      step_id = "WATERBODY_CODE"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(WATERBODY_NAME) <= 100,
      step_id = "WATERBODY_NAME"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(WATERBODY_TYPE) <= 50,
      step_id = "WATERBODY_TYPE"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(BEACH_PRESENT) <= 15,
      step_id = "BEACH_PRESENT"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(FISHERIES_INDEX_NUMBER) <= 75,
      step_id = "FISHERIES_INDEX_NUMBER"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(PUBLIC_WATER_SUPPLY) <= 15,
      step_id = "PUBLIC_WATER_SUPPLY"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(USGS_POND_NUMBER) <= 15,
      step_id = "USGS_POND_NUMBER"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(WATERBODY_ALTERNATE_NAME) <= 100,
      step_id = "WATERBODY_ALTERNATE_NAME"
    ) |>
    pointblank::col_vals_not_null(
      columns = c(
        "BEACH_PRESENT",
        "WATERBODY_CODE",
        "WATERBODY_NAME",
        "WATERBODY_TYPE",
        "FISHERIES_INDEX_NUMBER",
        "PUBLIC_WATER_SUPPLY",
        "USGS_POND_NUMBER",
        "WATERBODY_ALTERNATE_NAME"
      )
    ) |>
    pointblank::col_vals_in_set(
      columns = 'BEACH_PRESENT',
      set = c('not_applicable',
              "unknown",
              'FALSE',
              'TRUE')
    ) |>
    pointblank::col_vals_in_set(
      columns = 'PUBLIC_WATER_SUPPLY',
      set = c('not_applicable',
              "unknown",
              'FALSE',
              'TRUE')
    ) |>
    pointblank::col_vals_in_set(
      columns = 'WATERBODY_TYPE',
      set = c('river_stream',
              'lake',
              'outfall',
              'other')
    ) |>
    pointblank::rows_distinct(step_id = "checking rows are distinct") |>
    pointblank::rows_distinct(
      columns = WATERBODY_CODE,
      step_id = "Checking that SITE_CODE is unique"
    ) |>
    pointblank::interrogate(
      get_first_n = 50,
      progress = FALSE)


}


## PROJECT -----------------------------------------------------------------

val_project <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "PROJECT",
    label = "Expected Columns Exist",

  ) |>
    pointblank::col_exists(columns = c("PROJECT")) |>
    pointblank::col_exists(columns = c("PROJECT_TYPE")) |>
    pointblank::col_exists(columns = c("PROJECT_DESCRIPTION")) |>
    pointblank::col_is_character(
      columns = c("PROJECT",
                  "PROJECT_TYPE",
                  "PROJECT_DESCRIPTION"),
      step_id = "Checking character types"
    ) |>
    pointblank::col_count_match(
      count = 3,
      step_id = 'Check total columns'
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(PROJECT) <= 100,
      step_id = "PROJECT"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(PROJECT_DESCRIPTION) <= 256,
      step_id = "PROJECT_DESCRIPTION"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(PROJECT_TYPE) <= 100,
      step_id = "PROJECT_TYPE"
    ) |>
    pointblank::col_vals_not_null(
      columns = c(
        "PROJECT",
        "PROJECT_TYPE",
        "PROJECT_DESCRIPTION"
      )
    ) |>
    pointblank::col_vals_in_set(
      columns = 'PROJECT_TYPE',
      set = c('Biomonitoring',
              'CSLAP',
              'HAB',
              'HABs',
              'Historical RIBS',
              'LRNC',
              'Mohawk',
              'LCI',
              'Other',
              'PEERS',
              'Pre RIBS',
              'Probability Sampling',
              'PWS',
              'QAQC',
              'RAS',
              'Reference',
              'RIBS Intensive',
              'RIBS Routine',
              'RIBS Screening',
              'RMN',
              'Special Request',
              'Special Study',
              'TMDL',
              'Undefined',
              'WSNC',
              "Intensive",
              "Quality Control",
              "Regional Monitoring Network",
              "Screening",
              "Trend"),
      step_id = 'Check Project Type'
    ) |>
    pointblank::rows_distinct(step_id = "checking rows are distinct") |>
    pointblank::rows_distinct(
      columns = PROJECT,
      step_id = "Checking that PROJECT is unique"
    ) |>
    pointblank::interrogate(
      get_first_n = 50,
      progress = FALSE
    )


}

## SITE --------------------------------------------------------------------

val_site <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "SITE",
    label = "Expected Columns Exist"
  ) |>
    pointblank::col_exists(
      columns = c("COUNTY",
                  "ECOREGION_LEVEL_4",
                  "HORIZONTAL_DATUM",
                  "HORIZONTAL_METHOD",
                  "HUC12",
                  "LOCATION",
                  "LOCATION_TYPE",
                  "MUNICIPALITY",
                  "NUTRIENT_ECOREGION",
                  "SITE_CODE",
                  "SITE_DESCRIPTION",
                  "SUBBASIN",
                  "WATERBODY_CLASS",
                  "WATERBODY_CODE",
                  "WATERSHED_ID_NUMBER",
                  "WIPWL",
                  "BASIN",
                  "DEC_REGION",
                  "LATITUDE",
                  "LONGITUDE"),
      step_id = "Checking for expected columns"
    ) |>
    pointblank::col_is_character(
      columns = c(
        "BASIN",
        "COUNTY",
        "ECOREGION_LEVEL_4",
        "HORIZONTAL_DATUM",
        "HORIZONTAL_METHOD",
        "HUC12",
        "LOCATION",
        "LOCATION_TYPE",
        "MUNICIPALITY",
        "NUTRIENT_ECOREGION",
        "SITE_CODE",
        "SITE_DESCRIPTION",
        "SUBBASIN",
        "WATERBODY_CLASS",
        # "WATERBODY_CODE",
        "WATERSHED_ID_NUMBER",
        "WIPWL"
      ),
      step_id = "Checking character types"
    ) |>
    pointblank::col_is_numeric(
      columns = c(
        "BASIN",
        "WATERBODY_ID",
        "DEC_REGION",
        "LATITUDE",
        "LONGITUDE"
      ),
      step_id = "Checking numeric type"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(ECOREGION_LEVEL_4) <= 4,
      step_id = "ECOREGION_LEVEL_4"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SUBBASIN) == 4,
      step_id = "SUBBASIN"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(WIPWL) <= 9,
      step_id = "WIPWL"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(HUC12) <= 12,
      step_id = "HUC12"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(HORIZONTAL_DATUM) <= 20,
      step_id = "HORIZONTAL_DATUM"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(LOCATION) <= 20,
      step_id = "LOCATION"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(NUTRIENT_ECOREGION) <= 20,
      step_id = "NUTRIENT_ECOREGION"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(WATERBODY_CLASS) <= 20,
      step_id = "WATERBODY_CLASS"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(COUNTY) <= 25,
      step_id = "COUNTY"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(HORIZONTAL_METHOD) <= 50,
      step_id = "HORIZONTAL_METHOD"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(LOCATION_TYPE) <= 50,
      step_id = "LOCATION_TYPE"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(MUNICIPALITY) <= 50,
      step_id = "MUNICIPALITY"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(WATERBODY_CODE) <= 50,
      step_id = "WATERBODY_CODE"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SITE_CODE) <= 100,
      step_id = "SITE_CODE"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(WATERSHED_ID_NUMBER) <= 100,
      step_id = "WATERSHED_ID_NUMBER"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SITE_DESCRIPTION) <= 100,
      step_id = "SITE_DESCRIPTION"
    ) |>
    pointblank::col_vals_not_null(
      columns = c(
        "BASIN",
        "COUNTY",
        "ECOREGION_LEVEL_4",
        "HORIZONTAL_DATUM",
        "HORIZONTAL_METHOD",
        "HUC12",
        "LOCATION",
        "LOCATION_TYPE",
        "MUNICIPALITY",
        "NUTRIENT_ECOREGION",
        "SITE_CODE",
        "SITE_DESCRIPTION",
        "SUBBASIN",
        "WATERBODY_CLASS",
        # "WATERBODY_CODE",
        "WATERSHED_ID_NUMBER",
        "WIPWL"
      ),
      step_id = "Checking that columns do not contain NULLs/NAs"
    ) |>
    pointblank::rows_distinct(step_id = "checking rows are distinct") |>
    pointblank::rows_distinct(
      columns = SITE_CODE,
      step_id = "Checking that SITE_CODE is unique"
    ) |>
    pointblank::col_vals_in_set(
      columns = COUNTY,
      set = c("Adams (MA)",
              "Albany",
              "Allegany",
              "Arlington (VT)",
              "Bronx",
              "Broome",
              "Cattaraugus",
              "Cayuga",
              "Chautauqua",
              "Chemung",
              "Chenango",
              "Clinton",
              "Columbia",
              "Cortland",
              "Delaware",
              "Dutchess",
              "Duttonville (NJ)",
              "Erie",
              "Essex",
              "Franklin",
              "Fulton",
              "Genesee",
              "Greene",
              "Hamilton",
              "Herkimer",
              "Jefferson",
              "Kings",
              "Lackawaxen (PA)",
              "Lewis",
              "Livingston",
              "Madison",
              "Manchester Center (VT)",
              "Milanville (PA)",
              "Milford (PA)",
              "Monroe",
              "Montgomery",
              "Nassau",
              "New York",
              "Niagara",
              "North Adams (MA)",
              "North Bennington (VT)",
              "North East (PA)",
              "Oneida",
              "Onondaga",
              "Ontario",
              "Orange",
              "Orleans",
              "Osceola (PA)",
              "Oswego",
              "Otsego",
              "Pemberwick (CT)",
              "Pownal (VT)",
              "Putnam",
              "Queens",
              "Rensselaer",
              "Richmond",
              "Rockland",
              "Saint Lawrence",
              "Saratoga",
              "Sayre (PA)",
              "Schenectady",
              "Schoharie",
              "Schuyler",
              "Seneca",
              "Shohola (PA)",
              "St Lawrence",
              "St. Lawrence",
              "Stamford (CT)",
              "Steuben",
              "Suffolk",
              "Sullivan",
              "Tioga",
              "Tompkins",
              "Ulster",
              "unknown",
              "Upper Saddle River (NJ)",
              "Warren",
              "Washington",
              "Wayne",
              "Westchester",
              "Wharton (NJ)",
              "Williamstown (MA)",
              "Wyoming",
              "Yates"),
      step_id = "Checking COUNTY Set"
    ) |>
    pointblank::col_vals_in_set(
      columns = DEC_REGION,
      set = 1:9,
      step_id = "Checking DEC_REGION Set"
    ) |>
    pointblank::col_vals_in_set(
      columns = ECOREGION_LEVEL_4,
      set = c("58a",
              "58aa",
              "58ab",
              "58ac",
              "58ad",
              "58ae",
              "58af",
              "58ag",
              "58b",
              "58e",
              "58i",
              "58j",
              "58x",
              "58y",
              "58z",
              "59c",
              "59g",
              "59i",
              "60a",
              "60b",
              "60c",
              "60d",
              "60e",
              "60f",
              "61c",
              "62b",
              "62d",
              "64b",
              "64e",
              "64g",
              "67j",
              "67k",
              "67l",
              "67m",
              "83a",
              "83b",
              "83c",
              "83d",
              "83e",
              "83f",
              "84a",
              "84c",
              "unknown"),
      step_id = "Checking ECOREGION_LEVEL_4 Set"
    ) |>
    pointblank::col_vals_in_set(
      columns = HORIZONTAL_DATUM,
      set = c("NAD83", "WGS84"),
      step_id = "Checking HORIZONTAL_DATUM Set"
    ) |>
    pointblank::col_vals_in_set(
      columns = HORIZONTAL_METHOD,
      set = c("gps_unspecified",
              "gps_wide_area_augmentation_system",
              "interpolation",
              "interpolation_map",
              "interpolation_other"),
      step_id = "Checking HORIZONTAL_METHOD Set"
    ) |>
    pointblank::col_vals_in_set(
      columns = LOCATION_TYPE,
      set = c("centroid",
              "deep_hole",
              "inlet",
              "lake",
              "open_water",
              "other",
              "outfall_number",
              "outlet",
              "public_water_supply_facility",
              "river_mile",
              "shore",
              "shoreline"),
      step_id = "Checking LOCATION_TYPE Set"
    ) |>
    pointblank::col_vals_in_set(
      columns = NUTRIENT_ECOREGION,
      set = c("11",
              "14",
              "7",
              "8",
              "unknown"),
      step_id = "Checking NUTRIENT_ECOREGION Set"
    ) |>
    pointblank::col_vals_in_set(
      columns = WATERBODY_CLASS,
      set = c("A",
              "A-S",
              "A(T)",
              "A(TS)",
              "AA",
              "AA-S",
              "AA(T)",
              "AA(TS)",
              "B",
              "B(T)",
              "B(TS)",
              "C",
              "C(T)",
              "C(TS)",
              "CT",
              "D",
              "SB",
              "SC",
              "SC / B",
              "SC / C",
              "SD / B",
              "unknown")
      ,
      step_id = "Checking WATERBODY_CLASS Set"
    ) |>
    pointblank::interrogate(
      get_first_n = 50,
      progress = FALSE
    )

}


## PREVIOUS_SITE -----------------------------------------------------------




## EVENT -------------------------------------------------------------------

val_event <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "EVENT",
    label = "Expected Columns Exist"
  ) |>
    # Validate the Expected # of columns present. Helps to prevent extra columns
    # from being present.
    pointblank::col_count_match(
      count = 5,
      step_id = 'Check total columns'
    ) |>
    # Validate Expected Columns Present
    pointblank::col_exists(columns = c("EVENT_DATETIME")) |>
    pointblank::col_exists(columns = c("EVENT_ID")) |>
    pointblank::col_exists(columns = c("PROJECT")) |>
    pointblank::col_exists(columns = c("QAPP_ID")) |>
    pointblank::col_exists(columns = c("SITE_CODE")) |>
    # Validate Column Type
    pointblank::col_is_character(
      columns = c(
        "EVENT_ID",
        "QAPP_ID",
        "PROJECT",
        "SITE_CODE"
      ),
      step_id = "Checking character types"
    ) |>
    pointblank::col_is_posix(
      columns = "EVENT_DATETIME",
      step_id = "Checking datetime type"
    ) |>
    # Validate Column Charter Limits
    pointblank::col_vals_expr(
      expr = ~ nchar(EVENT_ID) <= 50,
      step_id = "EVENT_ID"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(PROJECT) <= 100,
      step_id = "PROJECT"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(QAPP_ID) <= 50,
      step_id = "QAPP_ID"
    ) |>
    # Ensure values are not NULL
    pointblank::col_vals_not_null(
      columns = c("EVENT_DATETIME",
                  "EVENT_ID",
                  "PROJECT",
                  "QAPP_ID",
                  "SITE_CODE"),
      step_id = "Checking that columns do not contain NULLs/NAs"
    ) |>
    # Ensure Distinct
    pointblank::rows_distinct(step_id = "checking rows are distinct") |>
    pointblank::rows_distinct(
      columns = EVENT_ID,
      step_id = "Checking that EVENT_ID is unique"
    ) |>
    # Check values within expected range
    pointblank::col_vals_between(
      columns = EVENT_DATETIME,
      left = as.POSIXct("1965-01-01 00:00:00"),
      right = Sys.time()
    ) |>
    pointblank::interrogate(get_first_n = 50, progress = FALSE)
}

## SAMPLE_DELIVERY_GROUP ---------------------------------------------------

val_sdg <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "SAMPLE_DELIVERY_GROUP",
    actions = pointblank::action_levels(warn_at = 1)
  ) |>
    pointblank::col_exists(columns = c("SAMPLE_DELIVERY_GROUP")) |>
    pointblank::col_is_character(
      columns = c("SAMPLE_DELIVERY_GROUP"),
      step_id = "Checking character types"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_DELIVERY_GROUP) <= 50,
      step_id = "SAMPLE_DELIVERY_GROUP"
    ) |>
    pointblank::col_vals_not_null(
      columns = c(
        "SAMPLE_DELIVERY_GROUP"
      )
    ) |>
    pointblank::rows_distinct(step_id = "checking rows are distinct") |>
    pointblank::interrogate(
      get_first_n = 50,
      progress = FALSE)
}

## SAMPLE ------------------------------------------------------------------

val_sample <- function(df) {

  pointblank::create_agent(
    tbl = df,
    tbl_name = "SAMPLE",
    label = "Expected Columns Exist"
  ) |>
    # Check only the number of expected columns present. Mainly important for
    # checking that extra coluns are not present.
    pointblank::col_count_match(
      count = 17,
      step_id = 'Check total columns'
    ) |>
    # Check that expected columns exist.
    pointblank::col_exists(columns = c("EVENT_ID")) |>
    pointblank::col_exists(columns = c("REPLICATE")) |>
    pointblank::col_exists(columns = c("SAMPLE_CREW")) |>
    pointblank::col_exists(columns = c("SAMPLE_DELIVERY_GROUP")) |>
    pointblank::col_exists(columns = c("SAMPLE_LAB")) |>
    pointblank::col_exists(columns = c("SAMPLE_LOCATION")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD_DESCRIPTION")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD_REFERENCE")) |>
    pointblank::col_exists(columns = c("SAMPLE_NAME")) |>
    pointblank::col_exists(columns = c("SAMPLE_ORGANIZATION")) |>
    pointblank::col_exists(columns = c("SAMPLE_SOURCE")) |>
    pointblank::col_exists(columns = c("SAMPLE_TYPE")) |>
    pointblank::col_exists(columns = c("SAMPLE_COMMENT")) |>
    pointblank::col_exists(columns = c("SAMPLE_DEPTH_METERS")) |>
    pointblank::col_exists(columns = c("SAMPLE_LATITUDE")) |>
    pointblank::col_exists(columns = c("SAMPLE_LONGITUDE")) |>
    # Check column types
    pointblank::col_is_character(
      columns = c(
        "EVENT_ID",
        "SAMPLE_DELIVERY_GROUP",
        "REPLICATE",
        "SAMPLE_CREW",
        "SAMPLE_LAB",
        "SAMPLE_LOCATION",
        "SAMPLE_METHOD",
        "SAMPLE_METHOD_DESCRIPTION",
        "SAMPLE_METHOD_REFERENCE",
        "SAMPLE_NAME",
        "SAMPLE_ORGANIZATION",
        "SAMPLE_SOURCE",
        "SAMPLE_TYPE",
        "SAMPLE_COMMENT"
      ),
      step_id = "Checking character types"
    ) |>
    pointblank::col_is_numeric(
      columns = c(
        "SAMPLE_DEPTH_METERS",
        "SAMPLE_LATITUDE",
        "SAMPLE_LONGITUDE"
      ),
      step_id = "Checking numeric type"
    ) |>
    # Check max character limit not exceeded.
    pointblank::col_vals_expr(
      expr = ~ nchar(EVENT_ID) <= 50,
      step_id = "EVENT_ID"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_CREW) <= 100,
      step_id = "SAMPLE_CREW"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(REPLICATE) <= 15,
      step_id = "REPLICATE"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_DELIVERY_GROUP) <= 50,
      step_id = "SAMPLE_DELIVERY_GROUP"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_LAB) <= 50,
      step_id = "SAMPLE_LAB"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_LOCATION) <= 50,
      step_id = "SAMPLE_LOCATION"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_METHOD) <= 50,
      step_id = "SAMPLE_METHOD"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_SOURCE) <= 50,
      step_id = "SAMPLE_SOURCE"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_TYPE) <= 50,
      step_id = "SAMPLE_TYPE"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_METHOD_REFERENCE) <= 100,
      step_id = "SAMPLE_METHOD_REFERENCE"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_NAME) <= 100,
      step_id = "SAMPLE_NAME"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_ORGANIZATION) <= 100,
      step_id = "SAMPLE_ORGANIZATION"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_METHOD_DESCRIPTION) <= 200,
      step_id = "SAMPLE_METHOD_DESCRIPTION"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(SAMPLE_COMMENT) <= 2000,
      step_id = "SAMPLE_COMMENT charater limit check"
    ) |>
    # Check not null.
    pointblank::col_vals_not_null(
      columns = c( "EVENT_ID",
                   "SAMPLE_DELIVERY_GROUP",
                   "REPLICATE",
                   "SAMPLE_CREW",
                   "SAMPLE_LAB",
                   "SAMPLE_LOCATION",
                   "SAMPLE_METHOD",
                   "SAMPLE_METHOD_DESCRIPTION",
                   "SAMPLE_METHOD_REFERENCE",
                   "SAMPLE_NAME",
                   "SAMPLE_ORGANIZATION",
                   "SAMPLE_SOURCE",
                   "SAMPLE_TYPE",
                   "SAMPLE_COMMENT"),
      step_id = "Checking that columns do not contain NULLs/NAs"
    ) |>
    # Check distinct.
    pointblank::rows_distinct(step_id = "checking rows are distinct") |>
    pointblank::rows_distinct(
      columns = c("EVENT_ID",
                  "REPLICATE",
                  "SAMPLE_CREW",
                  "SAMPLE_DELIVERY_GROUP",
                  "SAMPLE_LAB",
                  "SAMPLE_LOCATION",
                  "SAMPLE_METHOD",
                  "SAMPLE_METHOD_DESCRIPTION",
                  "SAMPLE_NAME",
                  "SAMPLE_ORGANIZATION",
                  "SAMPLE_SOURCE",
                  "SAMPLE_TYPE",
                  "SAMPLE_DEPTH_METERS"
      ),
      step_id = "Checking that columns used to create SAMPLE_ID have unique rows"
    ) |>
    # Check expected ranges/values
    pointblank::col_vals_between(
      columns = SAMPLE_DEPTH_METERS,
      left = 0,
      right = 190,
      na_pass = TRUE
    ) |>
    pointblank::col_vals_between(
      columns = SAMPLE_LATITUDE,
      left = 40.5,
      right = 47.5,
      na_pass = TRUE
    ) |>
    pointblank::col_vals_between(
      columns = SAMPLE_LONGITUDE,
      left = -80.5,
      right = -37.0,
      na_pass = TRUE
    ) |>
    pointblank::col_vals_in_set(
      columns = SAMPLE_LAB,
      set = c(
        "ALS",
        "ALS_C",
        "ALSRNY",
        "AquatTOX Research",
        "CASKEL",
        "CASROCH",
        "CREL",
        "DOH",
        "Eurofins Frontier Global Sciences, LLC",
        "Hale Creek Field Station",
        "MRBP",
        "not_applicable",
        "NYDEC21_WQX",
        "PACEBTR",
        "PEERS",
        "SBK",
        "STLBUF",
        "SUNYESF",
        "TALBURL",
        "UFI",
        "unknown"
      )
    ) |>
    pointblank::col_vals_in_set(
      columns = SAMPLE_ORGANIZATION,
      set = c("Canandaigua Lake Watershed Association",
              "Cayuga Lake Watershed Network",
              "Chautauqua Lake Association",
              "Citizens Statewide Lake Assessment Program (CSLAP)",
              "City of Albany Water Department",
              "County Department of Health (DOH)",
              "CSLAP",
              "dow_flhub",
              "dow_hrep",
              "dow_mohawk",
              "DOW_Mohawk",
              "DOW_RIBS_Region",
              "dow_smas",
              "DOW_SMAS",
              "DOW_SMAS_HISTORICAL",
              "Finger Lakes Institute",
              "Honeoye Lake",
              "Keuka Lake Association",
              "New York City Department of Environmental Protection",
              "New York City Parks",
              "New York State Office of Parks, Recreation and Historical Preservation",
              "New York State Department of Health",
              "NYSDEC",
              "NYSDEC Central Office",
              "NYSDEC Region 1",
              "NYSDEC Region 3",
              "NYSDEC Region 3A",
              "NYSDEC Region 3B",
              "NYSDEC Region 4",
              "NYSDEC Region 5",
              "NYSDEC Region 5A",
              "NYSDEC Region 6",
              "NYSDEC Region 6A",
              "NYSDEC Region 6B",
              "NYSDEC Region 7",
              "NYSDEC Region 8",
              "NYSDEC Region 8A",
              "NYSDEC Region 8B",
              "NYSDEC Region 9",
              "Other",
              "Otisco Lake Preservation Association",
              "Owasco Watershed Lake Association",
              "Parsons",
              "Parsons-GLC Contractor",
              "PEERS",
              "Public",
              "Seneca Lake Pure Waters Association",
              "Skaneateles Lake Association",
              "Stony Brook University",
              "SUNY College of Environmental Science and Forestry",
              "U.S. Geological Survey (USGS)",
              "unknown")
    ) |>
    pointblank::col_vals_in_set(
      columns = SAMPLE_TYPE,
      set = c(
        "cdubia_toxicity",
        "chemistry",
        "cslap_field",
        "hab",
        "hab_field",
        "hab_report",
        "hab_status",
        "habitat_assessment_high_gradient",
        "habitat_assessment_low_gradient",
        "habitat_descriptions",
        "insitu_chemistry",
        "lci_field",
        "macroinvertebrate_abundance",
        "macroinvertebrate_metrics",
        "macroinvertebrate_survey_high_gradient",
        "macroinvertebrate_survey_low_gradient",
        "pebble_count",
        "user_perception",
        "water_column")
    ) |>
    pointblank::interrogate( progress = FALSE)
}


## RESULT ------------------------------------------------------------------

val_result <- function(df) {
  pointblank::create_agent(tbl = df,
                           tbl_name = "RESULT",
                           label = "Expected Columns Exist"
                           # action = action_levels(warn_at = 0.1, stop_at = 1)
  ) |>
    pointblank::col_count_match(
      count = 46
    ) |>
    # SAMPLE Cols
    pointblank::col_exists(columns = c("EVENT_ID")) |>
    pointblank::col_exists(columns = c("REPLICATE")) |>
    pointblank::col_exists(columns = c("SAMPLE_LOCATION")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD")) |>
    pointblank::col_exists(columns = c("SAMPLE_SOURCE")) |>
    pointblank::col_exists(columns = c("SAMPLE_TYPE")) |>
    pointblank::col_exists(columns = c("SAMPLE_DEPTH_METERS")) |>
    pointblank::col_exists(columns = c("SAMPLE_CREW")) |>
    pointblank::col_exists(columns = c("SAMPLE_NAME")) |>
    pointblank::col_exists(columns = c("SAMPLE_LAB")) |>
    pointblank::col_exists(columns = c("SAMPLE_ORGANIZATION")) |>
    pointblank::col_exists(columns = c("SAMPLE_DELIVERY_GROUP")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD_DESCRIPTION")) |>
    # Parameter Cols
    pointblank::col_exists(columns = c("MATRIX")) |>
    pointblank::col_exists(columns = c("FRACTION")) |>
    pointblank::col_exists(columns = c("METHOD_SPECIATION")) |>
    pointblank::col_exists(columns = c("PARAMETER_NAME")) |>
    pointblank::col_exists(columns = c("UNIT")) |>
    pointblank::col_exists(columns = c("CASRN")) |>
    # Result Cols
    pointblank::col_exists(columns = "ANALYSIS_DATETIME") |>
    pointblank::col_exists(columns = "DETECT_FLAG") |>
    pointblank::col_exists(columns = "DETECTION_LIMIT_UNIT") |>
    pointblank::col_exists(columns = "DILUTION_FACTOR") |>
    pointblank::col_exists(columns = "FINAL_VOLUME") |>
    pointblank::col_exists(columns = "FINAL_VOLUME_UNIT") |>
    pointblank::col_exists(columns = "LAB_ANALYTICAL_METHOD") |>
    pointblank::col_exists(columns = "LAB_QUALIFIER") |>
    pointblank::col_exists(columns = "LAB_SAMPLE_ID") |>
    pointblank::col_exists(columns = "LAB_VALIDATION_LEVEL") |>
    pointblank::col_exists(columns = "METHOD_DETECTION_LIMIT") |>
    pointblank::col_exists(columns = "PREP_DATETIME") |>
    pointblank::col_exists(columns = "PREP_METHOD") |>
    pointblank::col_exists(columns = "QUALIFIER_SOURCE") |>
    pointblank::col_exists(columns = "QUANTITATION_LIMIT") |>
    pointblank::col_exists(columns = "REPORTABLE_RESULT") |>
    pointblank::col_exists(columns = "REPORTING_DETECTION_LIMIT") |>
    pointblank::col_exists(columns = "RESULT_CATEGORY") |>
    pointblank::col_exists(columns = "RESULT_COMMENT") |>
    pointblank::col_exists(columns = "RESULT_QUALIFIER") |>
    pointblank::col_exists(columns = "RESULT_QUALIFIER_NOTE") |>
    pointblank::col_exists(columns = "RESULT_TYPE_CODE") |>
    pointblank::col_exists(columns = "RESULT_VALUE") |>
    pointblank::col_exists(columns = "SUBSAMPLE_AMOUNT") |>
    pointblank::col_exists(columns = "SUBSAMPLE_AMOUNT_UNIT") |>
    pointblank::col_exists(columns = "TEST_TYPE") |>
    # Check column type
    pointblank::col_is_character(
      columns = c(
        # Sample Cols
        "EVENT_ID",
        "REPLICATE",
        "SAMPLE_LOCATION",
        "SAMPLE_METHOD",
        "SAMPLE_SOURCE",
        "SAMPLE_TYPE",
        "SAMPLE_CREW",
        "SAMPLE_NAME",
        "SAMPLE_LAB",
        "SAMPLE_ORGANIZATION",
        "SAMPLE_DELIVERY_GROUP",
        "SAMPLE_METHOD_DESCRIPTION",
        # Parameter cols
        "MATRIX",
        "FRACTION",
        "METHOD_SPECIATION",
        "PARAMETER_NAME",
        "UNIT",
        "CASRN",
        "DETECT_FLAG",
        "DETECTION_LIMIT_UNIT",
        "FINAL_VOLUME_UNIT",
        "LAB_ANALYTICAL_METHOD",
        "LAB_QUALIFIER",
        "LAB_SAMPLE_ID",
        "LAB_VALIDATION_LEVEL",
        "PREP_METHOD",
        "QUALIFIER_SOURCE",
        "REPORTABLE_RESULT",
        "RESULT_CATEGORY",
        "RESULT_COMMENT",
        "RESULT_QUALIFIER_NOTE",
        "RESULT_TYPE_CODE",
        "SUBSAMPLE_AMOUNT_UNIT",
        "TEST_TYPE",
        "RESULT_QUALIFIER"
      ),
      step_id = "Checking character types"
    ) |>
    pointblank::col_is_numeric(
      columns = c(
        "SAMPLE_DEPTH_METERS",
        "DILUTION_FACTOR",
        "FINAL_VOLUME",
        "METHOD_DETECTION_LIMIT",
        "QUANTITATION_LIMIT",
        "REPORTING_DETECTION_LIMIT",
        "RESULT_VALUE",
        "SUBSAMPLE_AMOUNT"
      ),
      step_id = "Checking numeric type"
    ) |>
    pointblank::col_is_posix(columns = c("ANALYSIS_DATETIME", "PREP_DATETIME")) |>
    pointblank::col_vals_not_null(
      columns = c(
        # Sample Cols
        "EVENT_ID",
        "REPLICATE",
        "SAMPLE_LOCATION",
        "SAMPLE_METHOD",
        "SAMPLE_SOURCE",
        "SAMPLE_TYPE",
        "SAMPLE_CREW",
        "SAMPLE_NAME",
        "SAMPLE_LAB",
        "SAMPLE_ORGANIZATION",
        "SAMPLE_DELIVERY_GROUP",
        "SAMPLE_METHOD_DESCRIPTION",
        # Parameter cols
        "MATRIX",
        "FRACTION",
        "METHOD_SPECIATION",
        "PARAMETER_NAME",
        "UNIT",
        "CASRN",
        "DETECT_FLAG",
        "DETECTION_LIMIT_UNIT",
        "FINAL_VOLUME_UNIT",
        "LAB_ANALYTICAL_METHOD",
        "LAB_QUALIFIER",
        "LAB_SAMPLE_ID",
        "LAB_VALIDATION_LEVEL",
        "PREP_METHOD",
        "QUALIFIER_SOURCE",
        "REPORTABLE_RESULT",
        "RESULT_CATEGORY",
        "RESULT_COMMENT",
        "RESULT_QUALIFIER_NOTE",
        "RESULT_TYPE_CODE",
        "SUBSAMPLE_AMOUNT_UNIT",
        "TEST_TYPE",
        "RESULT_QUALIFIER"
      )
    ) |>
    # Check character limits.
    pointblank::col_vals_expr(expr = ~ nchar(RESULT_CATEGORY) <= 3000,
                              step_id = "RESULT_CATEGORY") |>
    pointblank::col_vals_expr(expr = ~ nchar(RESULT_COMMENT) <= 2000,
                              step_id = "RESULT_COMMENT") |>
    pointblank::col_vals_expr(expr = ~ nchar(RESULT_QUALIFIER) <= 10,
                              step_id = "RESULT_QUALIFIER") |>
    pointblank::col_vals_expr(expr = ~ nchar(RESULT_QUALIFIER_NOTE) <= 2000,
                              step_id = "RESULT_QUALIFIER_NOTE") |>
    pointblank::col_vals_expr(expr = ~ nchar(DETECT_FLAG) <= 50,
                              step_id = "DETECT_FLAG") |>
    pointblank::col_vals_expr(expr = ~ nchar(LAB_VALIDATION_LEVEL) <= 50,
                              step_id = "LAB_VALIDATION_LEVEL") |>
    pointblank::col_vals_expr(expr = ~ nchar(PREP_METHOD) <= 50,
                              step_id = "PREP_METHOD") |>
    pointblank::col_vals_expr(expr = ~ nchar(QUALIFIER_SOURCE) <= 50,
                              step_id = "QUALIFIER_SOURCE") |>
    pointblank::col_vals_expr(expr = ~ nchar(TEST_TYPE) <= 50,
                              step_id = "TEST_TYPE") |>
    pointblank::col_vals_expr(expr = ~ nchar(DETECTION_LIMIT_UNIT) <= 25,
                              step_id = "DETECTION_LIMIT_UNIT") |>
    pointblank::col_vals_expr(expr = ~ nchar(FINAL_VOLUME_UNIT) <= 25,
                              step_id = "FINAL_VOLUME_UNIT") |>
    pointblank::col_vals_expr(expr = ~ nchar(LAB_QUALIFIER) <= 25,
                              step_id = "LAB_QUALIFIER") |>
    pointblank::col_vals_expr(expr = ~ nchar(LAB_SAMPLE_ID) <= 25,
                              step_id = "LAB_SAMPLE_ID") |>
    pointblank::col_vals_expr(expr = ~ nchar(REPORTABLE_RESULT) <= 15,
                              step_id = "REPORTABLE_RESULT") |>
    pointblank::col_vals_expr(expr = ~ nchar(RESULT_TYPE_CODE) <= 15,
                              step_id = "RESULT_TYPE_CODE") |>
    pointblank::col_vals_expr(expr = ~ nchar(SUBSAMPLE_AMOUNT_UNIT) <= 15,
                              step_id = "SUBSAMPLE_AMOUNT_UNIT") |>
    # Check for distinct rows.
    pointblank::rows_distinct(step_id = "Checking that all rows are unique") |>
    pointblank::rows_distinct(columns = c("EVENT_ID",
                                          "REPLICATE",
                                          "SAMPLE_LOCATION",
                                          "SAMPLE_METHOD",
                                          "SAMPLE_SOURCE",
                                          "SAMPLE_TYPE",
                                          "SAMPLE_DEPTH_METERS",
                                          "SAMPLE_CREW",
                                          "SAMPLE_NAME",
                                          "SAMPLE_LAB",
                                          "SAMPLE_ORGANIZATION",
                                          "SAMPLE_DELIVERY_GROUP",
                                          "SAMPLE_METHOD_DESCRIPTION",
                                          "MATRIX",
                                          "FRACTION",
                                          "METHOD_SPECIATION",
                                          "PARAMETER_NAME",
                                          "UNIT",
                                          "CASRN")) |>
    pointblank::interrogate(get_first_n = 50, progress = FALSE)
}


## QUALITY_CONTROL ---------------------------------------------------------
val_quality_control <- function(df) {
  pointblank::create_agent(tbl = df,
                           tbl_name = "QUALITY_CONTROL",
                           # action = action_levels(warn_at = 0.1, stop_at = 1)
  ) |>
    pointblank::col_count_match(
      count = 53
    ) |>
    # SAMPLE Cols
    pointblank::col_exists(columns = c("SAMPLE_SOURCE")) |>
    pointblank::col_exists(columns = c("SAMPLE_TYPE")) |>
    pointblank::col_exists(columns = c("SAMPLE_NAME")) |>
    # Parameter Cols
    pointblank::col_exists(columns = c("MATRIX")) |>
    pointblank::col_exists(columns = c("FRACTION")) |>
    pointblank::col_exists(columns = c("METHOD_SPECIATION")) |>
    pointblank::col_exists(columns = c("PARAMETER_NAME")) |>
    pointblank::col_exists(columns = c("UNIT")) |>
    pointblank::col_exists(columns = c("CASRN")) |>
    # Result Cols
    pointblank::col_exists(columns = "ANALYSIS_DATETIME") |>
    pointblank::col_exists(columns = "DETECT_FLAG") |>
    pointblank::col_exists(columns = "DETECTION_LIMIT_UNIT") |>
    pointblank::col_exists(columns = "DILUTION_FACTOR") |>
    pointblank::col_exists(columns = "FINAL_VOLUME") |>
    pointblank::col_exists(columns = "FINAL_VOLUME_UNIT") |>
    pointblank::col_exists(columns = "LAB_ANALYTICAL_METHOD") |>
    pointblank::col_exists(columns = "LAB_QUALIFIER") |>
    pointblank::col_exists(columns = "LAB_SAMPLE_ID") |>
    pointblank::col_exists(columns = "LAB_VALIDATION_LEVEL") |>
    pointblank::col_exists(columns = "METHOD_DETECTION_LIMIT") |>
    pointblank::col_exists(columns = "PREP_DATETIME") |>
    pointblank::col_exists(columns = "PREP_METHOD") |>
    pointblank::col_exists(columns = "QUALIFIER_SOURCE") |>
    pointblank::col_exists(columns = "QUANTITATION_LIMIT") |>
    pointblank::col_exists(columns = "REPORTABLE_RESULT") |>
    pointblank::col_exists(columns = "REPORTING_DETECTION_LIMIT") |>
    pointblank::col_exists(columns = "RESULT_COMMENT") |>
    pointblank::col_exists(columns = "RESULT_QUALIFIER") |>
    pointblank::col_exists(columns = "RESULT_QUALIFIER_NOTE") |>
    pointblank::col_exists(columns = "RESULT_TYPE_CODE") |>
    pointblank::col_exists(columns = "RESULT_VALUE") |>
    pointblank::col_exists(columns = "SUBSAMPLE_AMOUNT") |>
    pointblank::col_exists(columns = "SUBSAMPLE_AMOUNT_UNIT") |>
    pointblank::col_exists(columns = "TEST_TYPE") |>
    pointblank::col_exists(columns = "QC_DUP_ORIGINAL_CONCENTRATION") |>
    pointblank::col_exists(columns = "QC_DUP_SPIKE_ADDED") |>
    pointblank::col_exists(columns = "QC_DUP_SPIKE_MEASURED") |>
    pointblank::col_exists(columns = "QC_DUP_SPIKE_RECOVERY") |>
    pointblank::col_exists(columns = "QC_DUP_SPIKE_STATUS") |>
    pointblank::col_exists(columns = "QC_LEVEL") |>
    pointblank::col_exists(columns = "QC_ORIGINAL_CONCENTRATION") |>
    pointblank::col_exists(columns = "QC_RPD") |>
    pointblank::col_exists(columns = "QC_RPD_CL") |>
    pointblank::col_exists(columns = "QC_RPD_STATUS") |>
    pointblank::col_exists(columns = "QC_SPIKE_ADDED") |>
    pointblank::col_exists(columns = "QC_SPIKE_LCL") |>
    pointblank::col_exists(columns = "QC_SPIKE_MEASURED") |>
    pointblank::col_exists(columns = "QC_SPIKE_RECOVERY") |>
    pointblank::col_exists(columns = "QC_SPIKE_STATUS") |>
    pointblank::col_exists(columns = "QC_SPIKE_UCL") |>
    # Check column type
    pointblank::col_is_character(
      columns = c(
        # Sample Cols
        "SAMPLE_SOURCE",
        "SAMPLE_TYPE",
        "SAMPLE_NAME",
        # Parameter cols
        "MATRIX",
        "FRACTION",
        "METHOD_SPECIATION",
        "PARAMETER_NAME",
        "UNIT",
        "CASRN",
        "DETECT_FLAG",
        "DETECTION_LIMIT_UNIT",
        "FINAL_VOLUME_UNIT",
        "LAB_ANALYTICAL_METHOD",
        "LAB_QUALIFIER",
        "LAB_SAMPLE_ID",
        "LAB_VALIDATION_LEVEL",
        "PREP_METHOD",
        "QUALIFIER_SOURCE",
        "REPORTABLE_RESULT",
        "RESULT_COMMENT",
        "RESULT_QUALIFIER_NOTE",
        "RESULT_TYPE_CODE",
        "SUBSAMPLE_AMOUNT_UNIT",
        "TEST_TYPE",
        "RESULT_QUALIFIER",
        "QC_DUP_SPIKE_STATUS",
        "QC_LEVEL",
        "QC_RPD_STATUS",
        "QC_SPIKE_STATUS"
      ),
      step_id = "Checking character types"
    ) |>
    pointblank::col_is_numeric(
      columns = c(
        "DILUTION_FACTOR",
        "FINAL_VOLUME",
        "METHOD_DETECTION_LIMIT",
        "QUANTITATION_LIMIT",
        "REPORTING_DETECTION_LIMIT",
        "RESULT_VALUE",
        "SUBSAMPLE_AMOUNT",
        "QC_DUP_ORIGINAL_CONCENTRATION",
        "QC_DUP_SPIKE_ADDED",
        "QC_DUP_SPIKE_MEASURED",
        "QC_DUP_SPIKE_RECOVERY",
        "QC_ORIGINAL_CONCENTRATION",
        "QC_RPD",
        "QC_RPD_CL",
        "QC_SPIKE_ADDED",
        "QC_SPIKE_LCL",
        "QC_SPIKE_MEASURED",
        "QC_SPIKE_RECOVERY",
        "QC_SPIKE_UCL"
      ),
      step_id = "Checking numeric type"
    ) |>
    pointblank::col_is_posix(columns = c("ANALYSIS_DATETIME", "PREP_DATETIME")) |>
    pointblank::col_vals_not_null(
      columns = c(
        # Sample Cols
        "SAMPLE_SOURCE",
        "SAMPLE_TYPE",
        "SAMPLE_NAME",
        # Parameter cols
        "MATRIX",
        "FRACTION",
        "METHOD_SPECIATION",
        "PARAMETER_NAME",
        "UNIT",
        "CASRN",
        "DETECT_FLAG",
        "DETECTION_LIMIT_UNIT",
        "FINAL_VOLUME_UNIT",
        "LAB_ANALYTICAL_METHOD",
        "LAB_QUALIFIER",
        "LAB_SAMPLE_ID",
        "LAB_VALIDATION_LEVEL",
        "PREP_METHOD",
        "QUALIFIER_SOURCE",
        "REPORTABLE_RESULT",
        "RESULT_CATEGORY",
        "RESULT_COMMENT",
        "RESULT_QUALIFIER_NOTE",
        "RESULT_TYPE_CODE",
        "SUBSAMPLE_AMOUNT_UNIT",
        "TEST_TYPE",
        "RESULT_QUALIFIER"
      )
    ) |>
    # Check character limits.
    pointblank::col_vals_expr(expr = ~ nchar(RESULT_COMMENT) <= 2000,
                              step_id = "RESULT_COMMENT") |>
    pointblank::col_vals_expr(expr = ~ nchar(RESULT_QUALIFIER_NOTE) <= 2000,
                              step_id = "RESULT_QUALIFIER_NOTE") |>
    pointblank::col_vals_expr(expr = ~ nchar(DETECT_FLAG) <= 50,
                              step_id = "DETECT_FLAG") |>
    pointblank::col_vals_expr(expr = ~ nchar(LAB_VALIDATION_LEVEL) <= 50,
                              step_id = "LAB_VALIDATION_LEVEL") |>
    pointblank::col_vals_expr(expr = ~ nchar(PREP_METHOD) <= 50,
                              step_id = "PREP_METHOD") |>
    pointblank::col_vals_expr(expr = ~ nchar(QUALIFIER_SOURCE) <= 50,
                              step_id = "QUALIFIER_SOURCE") |>
    pointblank::col_vals_expr(expr = ~ nchar(TEST_TYPE) <= 50,
                              step_id = "TEST_TYPE") |>
    pointblank::col_vals_expr(expr = ~ nchar(DETECTION_LIMIT_UNIT) <= 25,
                              step_id = "DETECTION_LIMIT_UNIT") |>
    pointblank::col_vals_expr(expr = ~ nchar(FINAL_VOLUME_UNIT) <= 25,
                              step_id = "FINAL_VOLUME_UNIT") |>
    pointblank::col_vals_expr(expr = ~ nchar(LAB_QUALIFIER) <= 25,
                              step_id = "LAB_QUALIFIER") |>
    pointblank::col_vals_expr(expr = ~ nchar(LAB_SAMPLE_ID) <= 25,
                              step_id = "LAB_SAMPLE_ID") |>
    pointblank::col_vals_expr(expr = ~ nchar(REPORTABLE_RESULT) <= 10,
                              step_id = "REPORTABLE_RESULT") |>
    pointblank::col_vals_expr(expr = ~ nchar(RESULT_TYPE_CODE) <= 10,
                              step_id = "RESULT_TYPE_CODE") |>
    pointblank::col_vals_expr(expr = ~ nchar(SUBSAMPLE_AMOUNT_UNIT) <= 15,
                              step_id = "SUBSAMPLE_AMOUNT_UNIT") |>
    pointblank::col_vals_expr(expr = ~ nchar(QC_DUP_SPIKE_STATUS) <= 50,
                              step_id = "QC_DUP_SPIKE_STATUS") |>
    pointblank::col_vals_expr(expr = ~ nchar(QC_LEVEL) <= 50,
                              step_id = "QC_LEVEL") |>
    pointblank::col_vals_expr(expr = ~ nchar(QC_RPD_STATUS) <= 50,
                              step_id = "QC_RPD_STATUS") |>
    pointblank::col_vals_expr(expr = ~ nchar(QC_SPIKE_STATUS) <= 50,
                              step_id = "QC_SPIKE_STATUS") |>
    # Check for distinct rows.
    pointblank::rows_distinct(step_id = "Checking that all rows are unique") |>
    pointblank::rows_distinct(columns = c("SAMPLE_SOURCE",
                                          "SAMPLE_TYPE",
                                          "SAMPLE_NAME",
                                          "SAMPLE_DELIVERY_GROUP",
                                          "LAB_SAMPLE_ID",
                                          "MATRIX",
                                          "FRACTION",
                                          "METHOD_SPECIATION",
                                          "PARAMETER_NAME",
                                          "UNIT",
                                          "CASRN")) |>
    pointblank::interrogate(get_first_n = 50, progress = FALSE)
}

## RESULT_QUALIFIER --------------------------------------------------------
val_result_qualifier <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "RESULT_QUALIFIER",
    label = "Expected Columns Exist",

  ) |>
    pointblank::col_exists(columns = c("RESULT_QUALIFIER")) |>
    pointblank::col_exists(columns = c("RESULT_QUALIFIER_DESCRIPTION")) |>
    pointblank::col_is_character(
      columns = c("RESULT_QUALIFIER",
                  "RESULT_QUALIFIER_DESCRIPTION"),
      step_id = "Checking character types"
    ) |>
    pointblank::col_count_match(
      count = 2,
      step_id = 'Check total columns'
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(RESULT_QUALIFIER) <= 10,
      step_id = "RESULT_QUALIFIER"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(RESULT_QUALIFIER_DESCRIPTION) <= 200,
      step_id = "RESULT_QUALIFIER_DESCRIPTION"
    ) |>
    pointblank::col_vals_not_null(
      columns = c(
        "RESULT_QUALIFIER",
        "RESULT_QUALIFIER_DESCRIPTION"
      )
    ) |>
    pointblank::rows_distinct(step_id = "checking rows are distinct") |>
    pointblank::rows_distinct(
      columns = RESULT_QUALIFIER,
      step_id = "Checking that RESULT_QUALIFIER is unique"
    ) |>
    pointblank::interrogate(
      get_first_n = 50,
      progress = FALSE
    )
}


## PARAMETER ---------------------------------------------------------------

val_parameter <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "PARAMETER",
    label = "Expected Columns Exist",
  ) |>
    pointblank::col_exists(columns = c("PARAMETER_NAME")) |>
    pointblank::col_exists(columns = c("CASRN")) |>
    pointblank::col_exists(columns = c("FRACTION")) |>
    pointblank::col_exists(columns = c("MATRIX")) |>
    pointblank::col_exists(columns = c("METHOD_SPECIATION")) |>
    pointblank::col_exists(columns = c("UNIT")) |>
    pointblank::col_is_character(
      columns = c(
        "PARAMETER_NAME",
        "CASRN",
        "FRACTION",
        "MATRIX",
        "METHOD_SPECIATION",
        "UNIT"),
      step_id = "Checking character types"
    ) |>
    pointblank::col_count_match(
      count = 6,
      step_id = 'Check total columns'
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(PARAMETER_NAME) <= 100,
      step_id = "PARAMETER_NAME"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(CASRN) <= 50,
      step_id = "CASRN"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(FRACTION) <= 50,
      step_id = "FRACTION"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(MATRIX) <= 50,
      step_id = "MATRIX"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(METHOD_SPECIATION) <= 50,
      step_id = "METHOD_SPECIATION"
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(UNIT) <= 50,
      step_id = "UNIT"
    ) |>
    pointblank::col_vals_not_null(
      columns = c(
        "PARAMETER_NAME",
        "CASRN",
        "FRACTION",
        "MATRIX",
        "METHOD_SPECIATION",
        "UNIT"
      )
    ) |>
    pointblank::col_vals_in_set(
      columns = 'FRACTION',
      set = c('not_applicable',
              'dissolved',
              'total',
              'total_recoverable',
              'unknown'),
      label = 'Check fraction'
    ) |>
    pointblank::col_vals_in_set(
      columns = 'MATRIX',
      set = c('water',
              'not_applicable',
              'sediment'),
      label = 'Check matrix'
    ) |>
    pointblank::col_vals_in_set(
      columns = 'METHOD_SPECIATION',
      set = c('not_applicable',
              'as_caco3',
              'as_so4',
              'as_n',
              'as_p',
              'hydrogen_ion_h+'),
      label = 'Check method speciation'
    ) |>
    pointblank::col_vals_in_set(
      columns = 'UNIT',
      set = c("percent",
              "pg/L",
              "score_0-10",
              "raw",
              "boolean",
              "meters",
              "uS/cm",
              "celsius",
              "cm_per_second",
              "observed_taxa",
              "ordinal",
              "mg/L",
              "pH units",
              "rating_scale_category",
              "ppt",
              "NTU",
              "ug/L",
              "cfu/100mL",
              "umhos/cm",
              "category",
              "score_0-200",
              "score_0-100",
              "score_0-20",
              "rating_scale_0-10",
              "rating_scale_A-F",
              "count",
              "total_count",
              "index_score",
              "score",
              "ng/L",
              "cfs",
              "RFU",
              "ug/g",
              "color units",
              "celsius",
              "not_applicable",
              "binary",
              "1/cm",
              "mg/kg",
              "score_1-5",
              "m",
              "mV",
              "FNU",
              "ug/l"),
      label = 'Check units'
    ) |>
    pointblank::rows_distinct(step_id = "checking rows are distinct") |>
    pointblank::interrogate(
      get_first_n = 50,
      progress = FALSE
    )

}


## PARAMETER_NAME ----------------------------------------------------------


val_parameter_name <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "PARAMETER_NAME",
    label = "Expected Columns Exist",
  ) |>
    pointblank::col_exists(columns = c("PARAMETER_NAME")) |>
    pointblank::col_exists(columns = c("PARAMETER_DESCRIPTION")) |>
    pointblank::col_is_character(
      columns = c("PARAMETER_NAME",
                  "PARAMETER_DESCRIPTION"),
      step_id = "Checking character types"
    ) |>
    pointblank::col_count_match(
      count = 2,
      step_id = 'Check total columns'
    ) |>
    pointblank::col_vals_expr(
      expr = ~ nchar(PARAMETER_NAME) <= 100,
      step_id = "PARAMETER_NAME"
    ) |>
    pointblank::col_vals_not_null(
      columns = c(
        "PARAMETER_NAME",
        "PARAMETER_DESCRIPTION"
      )
    ) |>
    pointblank::rows_distinct(step_id = "checking rows are distinct") |>
    pointblank::interrogate(
      get_first_n = 50,
      progress = FALSE
    )

}



## TAXONOMY ----------------------------------------------------------------


## TAXONOMIC_ABUNDANCE -----------------------------------------------------


## TAXONOMIC_TRAIT ---------------------------------------------------------


## PREVIOUS_TAXON_NAME -----------------------------------------------------


## TAXONOMY_REFRENCE_JUNCTION ----------------------------------------------


## TAXONOMY_REFERENCE -----------------------------------------------------




# Validate Base Columns Present -------------------------------------------
validate_base_cols_present <- function(df, table_name) {
  switch(
    table_name,
    "SITE" = val_base_cols_site(df = df),
    "WATERBODY" = waterbody_col_exists_agent(df = df),
    "EVENT" = val_base_cols_event(df = df),
    "PROJECT" = val_base_cols_project(df = df),
    "SAMPLE_DELIVERY_GROUP" = sdg_col_exists_agent(df = df),
    "SAMPLE" = val_base_cols_sample(df = df),
    "RESULT" = val_base_cols_result(df = df),
    "RESULT_QUALIFIER" = RESULT_QUALIFIER_col_exists_agent(df = df),
    "PARAMETER" = val_base_cols_parameter(df = df),
    "PARAMETER_NAME" = pn_col_exists_agent(df = df),
    rlang::abort(glue::glue_col("You supplied: '{yellow table_name}' which is not a recognized table in the validate function"))
  )
}

val_base_cols_result <- function(df) {
  pointblank::create_agent(tbl = df,
                           tbl_name = "RESULT",
                           label = "Expected Columns Exist"
                           # action = action_levels(warn_at = 0.1, stop_at = 1)
  ) |>
    pointblank::col_exists(columns = "ANALYSIS_DATETIME") |>
    pointblank::col_exists(columns = "DETECT_FLAG") |>
    pointblank::col_exists(columns = "DETECTION_LIMIT_UNIT") |>
    pointblank::col_exists(columns = "DILUTION_FACTOR") |>
    pointblank::col_exists(columns = "FINAL_VOLUME") |>
    pointblank::col_exists(columns = "FINAL_VOLUME_UNIT") |>
    pointblank::col_exists(columns = "LAB_ANALYTICAL_METHOD") |>
    pointblank::col_exists(columns = "LAB_QUALIFIER") |>
    pointblank::col_exists(columns = "LAB_SAMPLE_ID") |>
    pointblank::col_exists(columns = "LAB_VALIDATION_LEVEL") |>
    pointblank::col_exists(columns = "METHOD_DETECTION_LIMIT") |>
    pointblank::col_exists(columns = "PREP_DATETIME") |>
    pointblank::col_exists(columns = "PREP_METHOD") |>
    pointblank::col_exists(columns = "QUALIFIER_SOURCE") |>
    pointblank::col_exists(columns = "QUANTITATION_LIMIT") |>
    pointblank::col_exists(columns = "REPORTABLE_RESULT") |>
    pointblank::col_exists(columns = "REPORTING_DETECTION_LIMIT") |>
    pointblank::col_exists(columns = "RESULT_CATEGORY") |>
    pointblank::col_exists(columns = "RESULT_COMMENT") |>
    pointblank::col_exists(columns = "RESULT_QUALIFIER") |>
    pointblank::col_exists(columns = "RESULT_QUALIFIER_NOTE") |>
    pointblank::col_exists(columns = "RESULT_TYPE_CODE") |>
    pointblank::col_exists(columns = "RESULT_VALUE") |>
    pointblank::col_exists(columns = "SUBSAMPLE_AMOUNT") |>
    pointblank::col_exists(columns = "SUBSAMPLE_AMOUNT_UNIT") |>
    pointblank::col_exists(columns = "TEST_TYPE") |>
    # SAMPLE_ID Composite key
    pointblank::col_exists(columns = c("EVENT_CODE")) |>
    pointblank::col_exists(columns = c("REPLICATE")) |>
    pointblank::col_exists(columns = c("SAMPLE_CREW")) |>
    pointblank::col_exists(columns = c("SAMPLE_DELIVERY_GROUP")) |>
    pointblank::col_exists(columns = c("SAMPLE_LAB")) |>
    pointblank::col_exists(columns = c("SAMPLE_LOCATION")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD_DESCRIPTION")) |>
    pointblank::col_exists(columns = c("SAMPLE_NAME")) |>
    pointblank::col_exists(columns = c("SAMPLE_ORGANIZATION")) |>
    pointblank::col_exists(columns = c("SAMPLE_SOURCE")) |>
    pointblank::col_exists(columns = c("SAMPLE_TYPE")) |>
    pointblank::col_exists(columns = c("SAMPLE_DEPTH_METERS")) |>
    # PARAMETER_ID Composite cols
    pointblank::col_exists(columns = c("PARAMETER_NAME")) |>
    pointblank::col_exists(columns = c("CASRN")) |>
    pointblank::col_exists(columns = c("FRACTION")) |>
    pointblank::col_exists(columns = c("MATRIX")) |>
    pointblank::col_exists(columns = c("METHOD_SPECIATION")) |>
    pointblank::col_exists(columns = c("UNIT")) |>


    pointblank::interrogate(get_first_n = 50, progress = FALSE)
}

val_base_cols_parameter <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "PARAMETER",
    label = "Expected Columns Exist",
  ) |>
    pointblank::col_exists(columns = c("PARAMETER_NAME")) |>
    pointblank::col_exists(columns = c("CASRN")) |>
    pointblank::col_exists(columns = c("FRACTION")) |>
    pointblank::col_exists(columns = c("MATRIX")) |>
    pointblank::col_exists(columns = c("METHOD_SPECIATION")) |>
    pointblank::col_exists(columns = c("UNIT")) |>
    pointblank::interrogate(
      progress = FALSE
    )
}

val_base_cols_sample <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "SAMPLE",
    label = "Expected Columns Exist"
  ) |>
    pointblank::col_exists(columns = c("EVENT_CODE")) |>
    pointblank::col_exists(columns = c("REPLICATE")) |>
    pointblank::col_exists(columns = c("SAMPLE_CREW")) |>
    pointblank::col_exists(columns = c("SAMPLE_DELIVERY_GROUP")) |>
    pointblank::col_exists(columns = c("SAMPLE_LAB")) |>
    pointblank::col_exists(columns = c("SAMPLE_LOCATION")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD_DESCRIPTION")) |>
    pointblank::col_exists(columns = c("SAMPLE_METHOD_REFERENCE")) |>
    pointblank::col_exists(columns = c("SAMPLE_NAME")) |>
    pointblank::col_exists(columns = c("SAMPLE_ORGANIZATION")) |>
    pointblank::col_exists(columns = c("SAMPLE_SOURCE")) |>
    pointblank::col_exists(columns = c("SAMPLE_TYPE")) |>
    pointblank::col_exists(columns = c("SAMPLE_COMMENT")) |>
    pointblank::col_exists(columns = c("SAMPLE_DEPTH_METERS")) |>
    pointblank::col_exists(columns = c("SAMPLE_LATITUDE")) |>
    pointblank::col_exists(columns = c("SAMPLE_LONGITUDE")) |>
    pointblank::interrogate(get_first_n = 50, progress = FALSE)
}

val_base_cols_project <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "PROJECT",
    label = "Expected Columns Exist",

  ) |>
    pointblank::col_exists(columns = c("PROJECT")) |>
    pointblank::col_exists(columns = c("PROJECT_TYPE")) |>
    pointblank::col_exists(columns = c("PROJECT_DESCRIPTION")) |>
    pointblank::interrogate(
      get_first_n = 50,
      progress = FALSE
    )
}



val_base_cols_event <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "EVENT",
    label = "Expected Columns Exist"
  ) |>
    pointblank::col_exists(columns = c("EVENT_DATETIME")) |>
    pointblank::col_exists(columns = c("EVENT_CODE")) |>
    pointblank::col_exists(columns = c("PROJECT")) |>
    pointblank::col_exists(columns = c("QAPP_ID")) |>
    pointblank::col_exists(columns = c("SITE_CODE")) |>
    pointblank::interrogate(get_first_n = 50, progress = FALSE)
}

val_base_cols_site <- function(df) {
  pointblank::create_agent(
    tbl = df,
    tbl_name = "SITE",
    label = "Expected Columns Exist"
  ) |>
    pointblank::col_exists(
      columns = c("COUNTY",
                  "ECOREGION_LEVEL_4",
                  "HORIZONTAL_DATUM",
                  "HORIZONTAL_METHOD",
                  "HUC12",
                  "LOCATION",
                  "LOCATION_TYPE",
                  "MUNICIPALITY",
                  "NUTRIENT_ECOREGION",
                  "SITE_CODE",
                  "SITE_DESCRIPTION",
                  "SUBBASIN",
                  "WATERBODY_CLASS",
                  "WATERBODY_CODE",
                  "WATERSHED_ID_NUMBER",
                  "WIPWL",
                  "BASIN",
                  "DEC_REGION",
                  "LATITUDE",
                  "LONGITUDE"),
      step_id = "Checking for expected columns"
    ) |>
    pointblank::interrogate(
      get_first_n = 50,
      progress = FALSE
    )
}
