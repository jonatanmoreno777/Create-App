library(shiny)
library(shinythemes)
library(leaflet)
library(plotly)
library(waiter)
library(shinyalert)
library(shinyjs)
library(tidyr)
library(renv)
library(dplyr)
library(lutz)
library(sf)
#library(weathercan)
library(DT)
library(naniar)
library(ncdf4)

# ==============================================================================
# CONFIGURACIÓN INICIAL Y OPCIONES
# ==============================================================================

# Configurar opciones de Shiny para archivos grandes
options(shiny.maxRequestSize = 3000*1024^2)  # 3 GB

# ==============================================================================
# MÓDULO: INTERFAZ DE USUARIO (UI)
# ==============================================================================

ui <- fluidPage(
  
  # === CONFIGURACIÓN DE WAITER/SPINNERS ===
  use_waiter(),
  waiter_show_on_load(html = spin_3k(), color = "black"),
  
  # === TEMA Y ESTILOS CSS ===
  theme = shinytheme("yeti"),
  tags$head(HTML("<title>Climate Data Extraction and Analysis Tool</title>")),
  
  tags$head(
    tags$style(HTML("
      .shiny-output-error-validation {
        color: red;
        font-weight: bold;
      }
      .info-box {
        background-color: #e8f4f8;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
        border-left: 4px solid #2196F3;
      }
      .download-section {
        background-color: #f9f9f9;
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #ddd;
        margin: 10px 0;
      }
      .upload-section {
        background-color: #e8f5e9;
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #4CAF50;
        margin: 10px 0;
      }
      .matrix-section {
        background-color: #fff3e0;
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #FF9800;
        margin: 10px 0;
      }
    "))
  ),
  
  # === ESTRUCTURA PRINCIPAL DE LA APLICACIÓN ===
  pageWithSidebar(
    
    headerPanel(title=div('Climate Data Extraction and Analysis Tool', 
                          img(src='', 
                              style = "float:right;"
                          )
    )),
    
    # === PANEL LATERAL (SIDEBAR) ===
    sidebarPanel(
      width = 3,
      
      # === SELECTOR DE FUENTE DE ESTACIONES ===
      radioButtons("station_source", h4("1. Select Station Source"),
                   choices = list("Pre-loaded Stations" = "preloaded",
                                  "Upload Custom Stations (CSV)" = "custom"),
                   selected = "preloaded"),
      
      # Panel para estaciones pre-cargadas
      conditionalPanel(
        condition = "input.station_source == 'preloaded'",
        selectInput(inputId = "main_selector",
                    label = h4('Select ID Type'),
                    choices = list('Climate ID', 'WMO ID', 'TC ID'),
                    selected = 'Climate ID'),
        
        selectizeInput("stn_id_input", label = h4("Enter Station ID"),
                       choices = c("Loading..."),
                       multiple= FALSE,
                       options = list(maxOptions = 10))
      ),
      
      # Panel para estaciones personalizadas
      conditionalPanel(
        condition = "input.station_source == 'custom'",
        div(class = "upload-section",
            h4("Upload Custom Stations CSV"),
            fileInput("custom_stations_file", "Select CSV file with stations",
                      accept = c(".csv"),
                      multiple = FALSE),
            
            helpText("CSV must contain columns: station_name, lat, lon"),
            helpText("Optional columns: station_id, climate_id, prov, elev"),
            
            # Selector de estación personalizada
            conditionalPanel(
              condition = "output.custom_stations_loaded",
              selectizeInput("custom_station_select", label = h4("Select Custom Station"),
                             choices = c("Upload CSV first"),
                             multiple = FALSE)
            )
        )
      ),
      
      # === INFORMACIÓN DE LA ESTACIÓN SELECCIONADA ===
      h4("2. Review Station Info"),
      h6(htmlOutput("stn_input_info")),
      h6(htmlOutput("stn_warning")),
      
      br(),
      
      # === SELECTOR DE INTERVALO DE DATOS ===
      selectInput("Intervals", h4("3. Select Data Interval"), ""),
      
      conditionalPanel(
        condition = "input.Intervals != 'month'",
        sliderInput("select_range", "Select Range:", min = 1800, max = 2100,
                    value = c(1900,2020), sep = ""),
      ),
      
      # === CARGA DE ARCHIVO NETCDF ===
      h4("4. Upload NetCDF File"),
      fileInput("nc_file_upload", "Select .nc file with daily data",
                accept = c(".nc"),
                multiple = FALSE),
      
      useShinyalert(force = TRUE),
      useShinyjs(),
      uiOutput("load_nc_button")
      
    ),
    
    # === PANEL PRINCIPAL CON PESTAÑAS ===
    mainPanel(
      tabsetPanel(
        
        # === PESTAÑA: INSTRUCCIONES (READ ME) ===
        tabPanel("Read Me", htmlOutput("README")),
        
        # === PESTAÑA: MAPA DE ESTACIONES ===
        tabPanel("Stations Map",
                 br(),
                 fluidRow(
                   column(6,
                          "Station info last updated: ", textOutput("info_date")
                   ),
                   column(6,
                          actionButton("update_meta", "Update Station Map from ECCC", 
                                       style = "float: right;")
                   )
                 ),
                 br(),
                 
                 # Información sobre estaciones cargadas
                 conditionalPanel(
                   condition = "input.station_source == 'custom' && output.custom_stations_loaded",
                   div(class = "info-box",
                       htmlOutput("custom_stations_info")
                   )
                 ),
                 br(),
                 leafletOutput("MapPlot", height = 600),
                 "Zoom into map to see station locations",
                 br(),
                 "Click on a location to see station info."
        ),
        
        # === PESTAÑA: TABLA DE DATOS CRUDOS ===
        tabPanel("Raw Data Table",
                 br(),
                 htmlOutput("data_preview_title"),
                 
                 # === SECCIÓN DE DESCARGA DE DATOS ===
                 conditionalPanel(
                   condition = "output.data_loaded && input.station_source == 'preloaded'",
                   div(class = "download-section",
                       h4("Download Data"),
                       fluidRow(
                         column(6,
                                downloadButton('download_daily', 'Download Daily Data (.csv)',
                                               style = "width: 100%; background-color: #4CAF50; color: white;")
                         ),
                         column(6,
                                downloadButton('download_monthly', 'Download Monthly Data (.csv)',
                                               style = "width: 100%; background-color: #2196F3; color: white;")
                         )
                       ),
                       br(),
                       # Opciones para datos mensuales
                       conditionalPanel(
                         condition = "input.Intervals == 'day'",
                         div(style = "background-color: #e8f4f8; padding: 10px; border-radius: 5px;",
                             h5("Monthly Data Options"),
                             selectInput("monthly_agg_function", "Aggregation Function:",
                                         choices = c("Mean (for temperature)" = "mean", 
                                                     "Sum (for precipitation)" = "sum", 
                                                     "Maximum" = "max", 
                                                     "Minimum" = "min"),
                                         selected = "sum"),
                             
                             checkboxGroupInput("monthly_vars", "Variables to include:",
                                                choices = c("All variables")),
                             
                             helpText("Note: Select specific variables or use 'All variables'"),
                             
                             # Botón para previsualizar datos mensuales
                             actionButton("preview_monthly", "Preview Monthly Data", 
                                          style = "width: 100%; background-color: #FF9800; color: white;")
                         )
                       )
                   )
                 ),
                 
                 br(),
                 
                 # Tabla para previsualizar datos mensuales
                 conditionalPanel(
                   condition = "input.preview_monthly > 0",
                   h4("Monthly Data Preview"),
                   DT::dataTableOutput("monthly_preview_table"),
                   br()
                 ),
                 
                 h4("Daily Data"),
                 DT::dataTableOutput("datatable"),
                 br()
        ),
        
        # === PESTAÑA: COMPLETITUD DE DATOS ===
        tabPanel("Data Completeness",
                 br(),
                 htmlOutput("data_preview_title_plot"),
                 br(),
                 h5("Figure is interactive, see top-right corner for available tools"),
                 plotlyOutput("pctmiss_plotly"),
                 br()
        ),
        
        # === PESTAÑA: RESUMEN DE DATOS ===
        tabPanel("Data Summary",
                 br(),
                 br(),
                 h5("This tab is underconstruction"),
                 br()
        ),
        
        # === PESTAÑA: MATRIZ DE ESTACIONES ===
        tabPanel("Station Matrix",
                 br(),
                 div(class = "matrix-section",
                     h4("Climate Data in Matrix Format"),
                     p("This view displays climate data with stations as columns for easy comparison across multiple locations."),
                     
                     conditionalPanel(
                       condition = "output.data_loaded",
                       fluidRow(
                         column(6,
                                uiOutput("matrix_variable_selector")
                         ),
                         column(6,
                                uiOutput("matrix_download_button")
                         )
                       ),
                       # NUEVO: Selector de tipo de matriz
                       fluidRow(
                         column(12,
                                radioButtons("matrix_type", "Matrix Format:",
                                             choices = list("Daily (stations as columns)" = "daily",
                                                            "Monthly (years × months)" = "monthly"),
                                             selected = "daily",
                                             inline = TRUE)
                         )
                       )
                     )
                 ),
                 
                 br(),
                 
                 # Mensaje cuando no hay datos
                 conditionalPanel(
                   condition = "!output.data_loaded",
                   div(class = "info-box",
                       h4("No Data Available"),
                       p("To use the matrix format:"),
                       tags$ul(
                         tags$li("Select 'Upload Custom Stations (CSV)' in the sidebar"),
                         tags$li("Upload a CSV file with multiple station coordinates"),
                         tags$li("Load NetCDF data for all locations"),
                         tags$li("The matrix will automatically generate with stations as columns")
                       ),
                       br(),
                       p("Example CSV format:"),
                       pre("station_name,lat,lon\nv1,-12.9,-76.5\nv2,-13.7,-75.7\nv3,-10.2,-77.3")
                   )
                 ),
                 
                 # Selector de estación para matriz mensual
                 conditionalPanel(
                   condition = "output.data_loaded && input.matrix_type == 'monthly'",
                   fluidRow(
                     column(6,
                            selectInput("selected_station_monthly", "Select Station to Display:",
                                        choices = c("Loading..."))
                     ),
                     column(6,
                            selectInput("monthly_agg_function_matrix", "Aggregation Function:",
                                        choices = c("Sum (precipitation)" = "sum",
                                                    "Mean (temperature)" = "mean",
                                                    "Maximum" = "max",
                                                    "Minimum" = "min"),
                                        selected = "sum")
                     )
                   )
                 ),
                 
                 # Tabla de matriz cuando hay datos
                 conditionalPanel(
                   condition = "output.data_loaded",
                   h4("Data Matrix Preview"),
                   conditionalPanel(
                     condition = "input.matrix_type == 'daily'",
                     p("Each station is shown as a separate column for easy comparison.")
                   ),
                   conditionalPanel(
                     condition = "input.matrix_type == 'monthly'",
                     p("Years as rows and months as columns for the selected station.")
                   ),
                   DT::dataTableOutput("matrix_datatable"),
                   br(),
                   div(class = "info-box",
                       h5("Matrix Format Features:"),
                       conditionalPanel(
                         condition = "input.matrix_type == 'daily'",
                         tags$ul(
                           tags$li("Stations as columns for easy comparison"),
                           tags$li("Fixed date column for horizontal scrolling"),
                           tags$li("Values rounded to 3 decimal places"),
                           tags$li("Download available in CSV format")
                         )
                       ),
                       conditionalPanel(
                         condition = "input.matrix_type == 'monthly'",
                         tags$ul(
                           tags$li("Years as rows, months as columns (ENE, FEB, MAR, etc.)"),
                           tags$li("Values rounded to 3 decimal places"),
                           tags$li("Download all stations in one CSV"),
                           tags$li("Select aggregation function (sum, mean, etc.)")
                         )
                       )
                   )
                 )
        )
      )
    )
  )
)

# ==============================================================================
# MÓDULO: SERVIDOR (SERVER) - CONFIGURACIÓN INICIAL
# ==============================================================================

server <- function(input, output, session) {
  
  # === CONFIGURACIÓN DE PAQUETES Y OPCIONES ===
  suppressPackageStartupMessages({
    library(dplyr)
    library(leaflet)
    library(parallel)
    library(future)
    library(promises)
    library(tidyr)
    library(lubridate)
    library(ncdf4)
    library(plotly)
    library(naniar)
    library(DT)
    library(waiter)
    library(shinyalert)
    library(shinyjs)
  })
  
  # Configurar plan paralelo simple
  plan(multisession)
  
  # Aumentar límite de subida
  options(shiny.maxRequestSize = 3000*1024^2)  # 3GB
  
  # === INICIALIZACIÓN DE SPINNERS ===
  spin_map <- Waiter$new("MapPlot", html = spin_3k(), color = "black")
  spin_datatable <- Waiter$new("datatable", html = spin_3k(), color = "black")
  spin_plot <- Waiter$new("pctmiss_plotly", html = spin_3k(), color = "black")
  spin_matrix <- Waiter$new("matrix_datatable", html = spin_3k(), color = "black")
  
  # === VALORES REACTIVOS PRINCIPALES ===
  nc_data <- reactiveVal(tibble())
  multi_nc_data <- reactiveVal(tibble())
  matrix_data <- reactiveVal(tibble())
  nc_data_title <- reactiveVal("No data loaded")
  available_variables <- reactiveVal(character(0))
  
  # Reactive values para estaciones personalizadas
  custom_stations <- reactiveVal(NULL)
  all_stations_data <- reactiveVal(NULL)
  
  # Reactive value para almacenar station_source actual
  current_station_source <- reactiveVal("preloaded")
  
  # NUEVO: Reactive value para la extensión del NetCDF
  nc_bbox <- reactiveVal(NULL)
  
  # NUEVO: Reactive value para matrices mensuales
  monthly_matrices <- reactiveVal(list())
  
  # === FUNCIÓN PARA EXTRAER LA EXTENSIÓN GEOGRÁFICA DEL NETCDF ===
  extract_nc_bbox <- function(nc_path) {
    tryCatch({
      nc_obj <- ncdf4::nc_open(nc_path)
      
      # Detectar dimensiones
      dim_names <- names(nc_obj$dim)
      
      # Buscar dimensiones de latitud y longitud
      lat_dim <- NULL
      lat_names <- c("lat", "latitude", "Latitude", "LAT", "LATITUDE", "y", "Y", "nav_lat")
      for(lat_name in lat_names) {
        if(lat_name %in% dim_names) {
          lat_dim <- lat_name
          break
        }
      }
      
      lon_dim <- NULL
      lon_names <- c("lon", "longitude", "Longitude", "LON", "LONGITUDE", "x", "X", "nav_lon")
      for(lon_name in lon_names) {
        if(lon_name %in% dim_names) {
          lon_dim <- lon_name
          break
        }
      }
      
      if(is.null(lat_dim) || is.null(lon_dim)) {
        ncdf4::nc_close(nc_obj)
        return(NULL)
      }
      
      # Obtener valores de coordenadas
      lats <- ncdf4::ncvar_get(nc_obj, lat_dim)
      lons <- ncdf4::ncvar_get(nc_obj, lon_dim)
      
      # Ajustar longitudes si están en 0-360
      if(max(lons) > 180) {
        lons <- ifelse(lons > 180, lons - 360, lons)
      }
      
      ncdf4::nc_close(nc_obj)
      
      # Crear bounding box
      bbox <- list(
        north = max(lats, na.rm = TRUE),
        south = min(lats, na.rm = TRUE),
        east = max(lons, na.rm = TRUE),
        west = min(lons, na.rm = TRUE),
        center_lat = mean(lats, na.rm = TRUE),
        center_lon = mean(lons, na.rm = TRUE)
      )
      
      return(bbox)
      
    }, error = function(e) {
      cat("Error extracting NetCDF bbox:", e$message, "\n")
      return(NULL)
    })
  }
  
  # ==============================================================================
  # MÓDULO: GESTIÓN DE ESTACIONES
  # ==============================================================================
  
  # Actualizar current_station_source cuando cambie el input
  observe({
    current_station_source(input$station_source)
  })
  
  # Output para conditional panels
  output$data_loaded <- reactive({
    nrow(nc_data()) > 0 || nrow(multi_nc_data()) > 0
  })
  outputOptions(output, "data_loaded", suspendWhenHidden = FALSE)
  
  output$custom_stations_loaded <- reactive({
    !is.null(custom_stations()) && nrow(custom_stations()) > 0
  })
  outputOptions(output, "custom_stations_loaded", suspendWhenHidden = FALSE)
  
  # Load Archived Station Data - PERÚ
  station_meta <- reactiveFileReader(intervalMillis = 1000, 
                                     session,
                                     filePath = "D:/Python_R/R/App/Data/stations_peru.rds", 
                                     readFunc = readRDS
  )
  
  # === CARGA DE ESTACIONES PERSONALIZADAS ===
  observeEvent(input$custom_stations_file, {
    req(input$custom_stations_file)
    
    tryCatch({
      # Leer el archivo CSV
      custom_data <- read.csv(input$custom_stations_file$datapath, 
                              stringsAsFactors = FALSE,
                              fileEncoding = "UTF-8")
      
      # Validar columnas mínimas requeridas
      required_cols <- c("station_name", "lat", "lon")
      if(!all(required_cols %in% names(custom_data))) {
        stop("CSV must contain columns: station_name, lat, lon")
      }
      
      # Asegurar que lat y lon sean numéricos
      custom_data$lat <- as.numeric(custom_data$lat)
      custom_data$lon <- as.numeric(custom_data$lon)
      
      # Agregar columnas opcionales si no existen
      if(!"station_id" %in% names(custom_data)) {
        custom_data$station_id <- 10000 + 1:nrow(custom_data)
      }
      if(!"climate_id" %in% names(custom_data)) {
        custom_data$climate_id <- paste0("CUSTOM", 1:nrow(custom_data))
      }
      if(!"prov" %in% names(custom_data)) {
        custom_data$prov <- "CUSTOM"
      }
      if(!"elev" %in% names(custom_data)) {
        custom_data$elev <- NA
      }
      if(!"interval" %in% names(custom_data)) {
        custom_data$interval <- "day"
      }
      
      # Guardar estaciones personalizadas
      custom_stations(custom_data)
      
      # Actualizar selector de estaciones personalizadas
      updateSelectizeInput(session, "custom_station_select",
                           choices = custom_data$station_name,
                           selected = custom_data$station_name[1])
      
      showNotification(paste("Loaded", nrow(custom_data), "custom stations"), 
                       type = "message")
      
    }, error = function(e) {
      shinyalert("Error", paste("Cannot load custom stations CSV:", e$message), 
                 type = "error")
    })
  })
  
  # === INFORMACIÓN DE ESTACIONES PERSONALIZADAS ===
  output$custom_stations_info <- renderUI({
    req(custom_stations())
    
    valid_coords <- custom_stations() %>% 
      filter(!is.na(lat), !is.na(lon))
    
    HTML(paste(
      "<strong>Custom Stations Loaded:</strong>", nrow(custom_stations()),
      "<br><strong>Stations with valid coordinates:</strong>", nrow(valid_coords),
      "<br><strong>Available stations:</strong>", 
      paste(custom_stations()$station_name, collapse = ", ")
    ))
  })
  
  # === DATOS COMBINADOS DE ESTACIONES ===
  observe({
    preloaded_data <- NULL
    custom_data <- NULL
    
    if(!is.null(station_meta()) && exists("stn", where = station_meta())) {
      preloaded_data <- station_meta()$stn %>%
        mutate(source = "preloaded") %>%
        filter(!is.na(lat), !is.na(lon))
    }
    
    if(!is.null(custom_stations())) {
      custom_data <- custom_stations() %>%
        mutate(source = "custom") %>%
        filter(!is.na(lat), !is.na(lon))
    }
    
    if(!is.null(preloaded_data) && nrow(preloaded_data) > 0 && 
       !is.null(custom_data) && nrow(custom_data) > 0) {
      combined_data <- bind_rows(preloaded_data, custom_data)
    } else if(!is.null(preloaded_data) && nrow(preloaded_data) > 0) {
      combined_data <- preloaded_data
    } else if(!is.null(custom_data) && nrow(custom_data) > 0) {
      combined_data <- custom_data
    } else {
      combined_data <- NULL
    }
    
    all_stations_data(combined_data)
  })
  
  # === OBTENER INFORMACIÓN DE LA ESTACIÓN SELECCIONADA ===
  selected_station_info <- reactive({
    tryCatch({
      if(current_station_source() == "preloaded") {
        req(input$stn_id_input, station_meta()$stn)
        
        if (input$main_selector == 'Climate ID'){
          result <- station_meta()$stn %>% 
            filter(climate_id == input$stn_id_input) %>% 
            slice(1)
        } else if (input$main_selector == 'WMO ID'){
          result <- station_meta()$stn %>% 
            filter(WMO_id == input$stn_id_input) %>% 
            slice(1)
        } else if (input$main_selector == 'TC ID'){
          result <- station_meta()$stn %>% 
            filter(TC_id == input$stn_id_input) %>% 
            slice(1)
        }
        
        if(!exists("result") || nrow(result) == 0) return(NULL)
        return(result)
        
      } else if(current_station_source() == "custom") {
        req(input$custom_station_select, custom_stations())
        
        result <- custom_stations() %>%
          filter(station_name == input$custom_station_select) %>%
          slice(1)
        
        if(nrow(result) == 0) return(NULL)
        return(result)
      }
      return(NULL)
    }, error = function(e) {
      return(NULL)
    })
  })
  
  # === ACTUALIZAR SELECTOR DE ESTACIONES PRE-CARGADAS ===
  observe({
    if(current_station_source() == "preloaded") {
      if (input$main_selector == 'Climate ID'){
        choices <- station_meta()$stn$climate_id[!is.na(station_meta()$stn$climate_id)]
      } else if (input$main_selector == 'WMO ID'){
        choices <- station_meta()$stn$WMO_id[!is.na(station_meta()$stn$WMO_id)]
      } else if (input$main_selector == 'TC ID'){
        choices <- station_meta()$stn$TC_id[!is.na(station_meta()$stn$TC_id)]
      }
      
      updateSelectizeInput(session, 'stn_id_input',
                           choices = choices,
                           selected = ifelse(length(choices) > 0, choices[1], NULL),
                           server = TRUE)
    }
  })
  
  # === ID ENTERED SIMPLIFICADO ===
  id_entered <- reactive({
    station_info <- selected_station_info()
    req(station_info)
    return(station_info$station_id)
  })
  
  # === ACTUALIZAR INTERVALOS DISPONIBLES ===
  observe({
    station_info <- selected_station_info()
    req(station_info)
    
    if("interval" %in% names(station_info)) {
      int_opts <- unique(station_info$interval)
      if(length(int_opts) > 0) {
        available_intervals <- intersect(c("hour", "day", "month"), int_opts)
        if(length(available_intervals) > 0) {
          updateSelectInput(session, "Intervals",
                            choices = available_intervals,
                            selected = ifelse("day" %in% available_intervals, "day", available_intervals[1]))
        }
      }
    }
  })
  
  # === INFORMACIÓN DE LA ESTACIÓN ===
  output$stn_input_info <- renderText({
    station <- selected_station_info()
    req(station)
    
    info_parts <- c(
      paste("<b>", station$station_name, "</b>"),
      paste("Lat: ", round(station$lat, 4), "° Lon: ", round(station$lon, 4), "°")
    )
    
    if(!is.null(station$elev) && !is.na(station$elev) && station$elev != "") {
      info_parts <- c(info_parts, paste("Elev: ", station$elev, "m"))
    }
    
    info_parts <- c(info_parts, 
                    paste("Source: ", if(current_station_source() == "preloaded") "Pre-loaded" else "Custom CSV"))
    
    paste(info_parts, collapse = "<br>")
  })
  
  output$stn_warning <- renderText({
    "Data will be extracted from NetCDF file for this location"
  })
  
  # === ACTUALIZAR RANGO TEMPORAL ===
  observe({
    if(input$Intervals != 'month'){
      req(selected_station_info())
      updateSliderInput(session, "select_range", value = c(1980, 2020),
                        min = 1950, max = 2100, step = 1)
    }
  })
  
  # ==============================================================================
  # MÓDULO: PROCESAMIENTO Y AGREGACIÓN DE DATOS
  # ==============================================================================
  
  # === FUNCIÓN PARA AGREGACIÓN MENSUAL ===
  aggregate_to_monthly <- function(daily_data, agg_fun = "mean", selected_vars = NULL) {
    
    meta_cols <- c("date", "station_name", "station_id", "climate_id", 
                   "lat", "lon", "prov", "grid_lat", "grid_lon", "day")
    
    climate_vars <- names(daily_data)[!names(daily_data) %in% meta_cols]
    
    if(!is.null(selected_vars) && !("All variables" %in% selected_vars)) {
      climate_vars <- intersect(climate_vars, selected_vars)
    }
    
    if(length(climate_vars) == 0) return(daily_data)
    
    monthly_data <- daily_data %>%
      mutate(
        year = as.numeric(format(date, "%Y")),
        month = as.numeric(format(date, "%m"))
      ) %>%
      group_by(station_name, station_id, climate_id, lat, lon, prov, 
               grid_lat, grid_lon, year, month) %>%
      summarise(
        across(
          all_of(climate_vars), 
          ~ {
            if(agg_fun == "sum") sum(., na.rm = TRUE)
            else if(agg_fun == "mean") mean(., na.rm = TRUE)
            else if(agg_fun == "max") max(., na.rm = TRUE)
            else if(agg_fun == "min") min(., na.rm = TRUE)
            else mean(., na.rm = TRUE)
          },
          .names = "{.col}"
        ),
        n_days = n(),
        .groups = "drop"
      ) %>%
      mutate(date = as.Date(paste(year, month, "01", sep = "-"))) %>%
      select(date, everything()) %>%
      arrange(date)
    
    monthly_data[climate_vars] <- lapply(monthly_data[climate_vars], function(x) {
      x[is.nan(x) | is.infinite(x)] <- NA
      return(x)
    })
    
    return(monthly_data)
  }
  
  # === DATOS MENSUALES REACTIVOS ===
  monthly_data <- reactive({
    req(nc_data())
    
    if(input$Intervals == "day" && nrow(nc_data()) > 0) {
      showNotification("Calculating monthly data...", type = "message")
      
      monthly_result <- aggregate_to_monthly(
        daily_data = nc_data(),
        agg_fun = input$monthly_agg_function,
        selected_vars = input$monthly_vars
      )
      
      showNotification("Monthly data calculated successfully!", type = "message")
      return(monthly_result)
    } else {
      return(NULL)
    }
  })
  
  # === PREVISUALIZACIÓN DE DATOS MENSUALES ===
  observeEvent(input$preview_monthly, {
    req(monthly_data())
    showNotification("Showing monthly data preview", type = "message")
  })
  
  output$monthly_preview_table <- DT::renderDataTable({
    req(monthly_data())
    
    monthly_data() %>%
      DT::datatable(
        rownames = FALSE,
        extensions = c('Buttons', 'Scroller'),
        options = list(
          dom = 'Bfrtip',
          buttons = list('colvis'),
          scrollX = TRUE,
          deferRender = TRUE,
          scrollY = 400,
          scroller = TRUE,
          pageLength = 12
        ),
        caption = "Monthly Data Preview"
      )
  })
  
  # === BOTONES DE DESCARGA ===
  output$download_daily <- downloadHandler(
    filename = function() {
      paste0("daily_climate_data_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
    },
    content = function(file) {
      write.csv(nc_data(), file, row.names = FALSE)
    }
  )
  
  output$download_monthly <- downloadHandler(
    filename = function() {
      paste0("monthly_climate_data_", input$monthly_agg_function, "_", 
             format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
    },
    content = function(file) {
      req(monthly_data())
      write.csv(monthly_data(), file, row.names = FALSE)
    }
  )
  
  # === ACTUALIZAR VARIABLES PARA AGREGACIÓN MENSUAL ===
  observe({
    req(nc_data())
    
    meta_cols <- c("date", "station_name", "station_id", "climate_id", 
                   "lat", "lon", "prov", "grid_lat", "grid_lon", 
                   "year", "month", "day")
    
    available_climate_vars <- names(nc_data())[!names(nc_data()) %in% meta_cols]
    
    if(length(available_climate_vars) > 0) {
      updateCheckboxGroupInput(session, "monthly_vars",
                               choices = c("All variables", available_climate_vars),
                               selected = "All variables")
    }
  })
  
  # ==============================================================================
  # MÓDULO: PROCESAMIENTO DE ARCHIVOS NETCDF
  # ==============================================================================
  
  # === FUNCIÓN PARA EXTRACCIÓN DE MÚLTIPLES ESTACIONES ===
  extract_multiple_stations <- function(nc_path, stations_data, selected_vars = NULL, start_year, end_year) {
    all_stations_data <- list()
    
    for(i in 1:nrow(stations_data)) {
      station <- stations_data[i, ]
      
      cat("Processing station:", station$station_name, "-", station$lat, ",", station$lon, "\n")
      
      tryCatch({
        # Extraer datos para esta estación
        station_data <- read_flexible_netcdf(
          nc_path = nc_path,
          lat = station$lat,
          lon = station$lon,
          station_name = station$station_name,
          climate_id = station$climate_id,
          prov = station$prov,
          station_id = station$station_id,
          selected_vars = selected_vars
        )
        
        # Filtrar por rango de años si es necesario
        if("year" %in% names(station_data)) {
          station_data <- station_data %>%
            filter(year >= start_year & year <= end_year)
        }
        
        all_stations_data[[i]] <- station_data
        
        cat("Successfully extracted data for", station$station_name, "-", nrow(station_data), "records\n")
        
      }, error = function(e) {
        cat("ERROR processing station", station$station_name, ":", e$message, "\n")
        # Crear un dataset vacío para esta estación
        empty_data <- tibble(
          date = as.Date(character()),
          station_name = character(),
          station_id = numeric(),
          climate_id = character(),
          lat = numeric(),
          lon = numeric(),
          prov = character(),
          grid_lat = numeric(),
          grid_lon = numeric(),
          year = numeric(),
          month = numeric(),
          day = numeric()
        )
        
        # Agregar columnas de variables si es necesario
        if(!is.null(selected_vars)) {
          for(var in selected_vars) {
            empty_data[[var]] <- numeric()
          }
        }
        
        all_stations_data[[i]] <- empty_data
      })
    }
    
    # Combinar todos los datos
    combined_data <- bind_rows(all_stations_data)
    return(combined_data)
  }
  
  # === FUNCIÓN PARA LEER NETCDF (VERSIÓN SIMPLIFICADA) ===
  read_flexible_netcdf <- function(nc_path, lat, lon, station_name, climate_id, prov, station_id, selected_vars = NULL) {
    
    nc_obj <- ncdf4::nc_open(nc_path)
    
    # Detectar dimensiones
    dim_names <- names(nc_obj$dim)
    
    lat_dim <- NULL
    lat_names <- c("lat", "latitude", "Latitude", "LAT", "LATITUDE", "y", "Y", "nav_lat")
    for(lat_name in lat_names) {
      if(lat_name %in% dim_names) {
        lat_dim <- lat_name
        break
      }
    }
    
    lon_dim <- NULL
    lon_names <- c("lon", "longitude", "Longitude", "LON", "LONGITUDE", "x", "X", "nav_lon")
    for(lon_name in lon_names) {
      if(lon_name %in% dim_names) {
        lon_dim <- lon_name
        break
      }
    }
    
    time_dim <- NULL
    time_names <- c("time", "Time", "TIME", "t", "T")
    for(time_name in time_names) {
      if(time_name %in% dim_names) {
        time_dim <- time_name
        break
      }
    }
    
    if(is.null(lat_dim) || is.null(lon_dim)) {
      ncdf4::nc_close(nc_obj)
      stop("No se pudieron detectar dimensiones de latitud/longitud")
    }
    
    # Obtener valores de dimensiones
    lats <- ncdf4::ncvar_get(nc_obj, lat_dim)
    lons <- ncdf4::ncvar_get(nc_obj, lon_dim)
    
    if(max(lons) > 180) {
      lons <- ifelse(lons > 180, lons - 360, lons)
    }
    
    times <- if(!is.null(time_dim)) {
      ncdf4::ncvar_get(nc_obj, time_dim)
    } else {
      1
    }
    
    # Encontrar índices más cercanos
    lat_idx <- which.min(abs(lats - lat))
    lon_idx <- which.min(abs(lons - lon))
    
    # Extraer variables
    variables_to_extract <- if(!is.null(selected_vars)) {
      intersect(selected_vars, names(nc_obj$var))
    } else {
      names(nc_obj$var)
    }
    
    if(length(variables_to_extract) == 0) {
      ncdf4::nc_close(nc_obj)
      stop("No hay variables disponibles para extraer")
    }
    
    # Extraer datos
    variables_data <- list()
    
    for(var_name in variables_to_extract) {
      var_array <- ncdf4::ncvar_get(nc_obj, var_name)
      var_dims <- length(dim(var_array))
      
      if(var_dims == 3) {
        if(dim(var_array)[1] == length(lons) && dim(var_array)[2] == length(lats)) {
          variables_data[[var_name]] <- var_array[lon_idx, lat_idx, ]
        } else if(dim(var_array)[1] == length(lats) && dim(var_array)[2] == length(lons)) {
          variables_data[[var_name]] <- var_array[lat_idx, lon_idx, ]
        } else {
          variables_data[[var_name]] <- rep(NA, length(times))
        }
      } else if(var_dims == 2) {
        if(dim(var_array)[1] == length(lons) && dim(var_array)[2] == length(lats)) {
          variables_data[[var_name]] <- rep(var_array[lon_idx, lat_idx], length(times))
        } else {
          variables_data[[var_name]] <- rep(NA, length(times))
        }
      } else {
        variables_data[[var_name]] <- rep(NA, length(times))
      }
    }
    
    # Manejar fechas
    if(!is.null(time_dim)) {
      time_units <- tryCatch(
        ncdf4::ncatt_get(nc_obj, time_dim)$units,
        error = function(e) NULL
      )
      
      if(!is.null(time_units) && grepl("days since", time_units)) {
        start_date <- as.Date(sub("days since ", "", time_units))
        dates <- start_date + times
      } else if(!is.null(time_units) && grepl("hours since", time_units)) {
        start_datetime <- as.POSIXct(sub("hours since ", "", time_units))
        dates <- start_datetime + times * 3600
        dates <- as.Date(dates)
      } else {
        dates <- seq.Date(as.Date("1970-01-01"), by = "day", length.out = length(times))
      }
    } else {
      dates <- seq.Date(Sys.Date() - length(times) + 1, Sys.Date(), by = "day")
    }
    
    # Crear data frame
    result <- data.frame(
      date = dates,
      station_name = station_name,
      station_id = station_id,
      climate_id = climate_id,
      lat = lat,
      lon = lon,
      prov = prov,
      grid_lat = lats[lat_idx],
      grid_lon = lons[lon_idx],
      year = as.numeric(format(dates, "%Y")),
      month = as.numeric(format(dates, "%m")),
      day = as.numeric(format(dates, "%d"))
    )
    
    # Agregar variables extraídas
    for(var_name in names(variables_data)) {
      result[[var_name]] <- round(variables_data[[var_name]], 2)
    }
    
    ncdf4::nc_close(nc_obj)
    
    return(result)
  }
  
  # === ACTUALIZAR VARIABLES DISPONIBLES DEL NETCDF ===
  observeEvent(input$nc_file_upload, {
    req(input$nc_file_upload)
    
    tryCatch({
      # NUEVO: Extraer la extensión geográfica para el mapa
      bbox <- extract_nc_bbox(input$nc_file_upload$datapath)
      nc_bbox(bbox)
      
      nc_obj <- ncdf4::nc_open(input$nc_file_upload$datapath)
      vars <- names(nc_obj$var)
      ncdf4::nc_close(nc_obj)
      
      available_variables(vars)
      
      # DEFINIR PRIORIDADES POR CATEGORÍA
      variable_priority <- list(
        precipitacion = c("pr", "precip", "precipitation", "ppt", "rain", "rainfall", "prp", "pcp", "variable"),
        temperatura_media = c("tas", "temp", "temperature", "tmean", "tavg"),
        temperatura_max = c("tasmax", "tmax", "temp_max", "temperature_max"),
        temperatura_min = c("tasmin", "tmin", "temp_min", "temperature_min"),
        humedad = c("hurs", "rh", "humidity", "relative_humidity"),
        radiacion = c("rsds", "srad", "solar_radiation", "radiation"),
        viento = c("sfcWind", "wind", "wind_speed", "ws"),
        presion = c("ps", "pressure", "surface_pressure"),
        evapotranspiracion = c("et", "evapotranspiration", "pet")
      )
      
      # BUSCAR VARIABLES EN ORDEN DE PRIORIDAD
      selected_var <- NULL
      
      # Primero buscar variables de precipitación
      for (precip_var in variable_priority$precipitacion) {
        if (precip_var %in% vars) {
          selected_var <- precip_var
          break
        }
      }
      
      # Si no hay precipitación, buscar temperatura media
      if (is.null(selected_var)) {
        for (temp_var in variable_priority$temperatura_media) {
          if (temp_var %in% vars) {
            selected_var <- temp_var
            break
          }
        }
      }
      
      # Si no hay temperatura media, buscar temperatura máxima
      if (is.null(selected_var)) {
        for (tmax_var in variable_priority$temperatura_max) {
          if (tmax_var %in% vars) {
            selected_var <- tmax_var
            break
          }
        }
      }
      
      # Si no hay temperatura máxima, buscar temperatura mínima
      if (is.null(selected_var)) {
        for (tmin_var in variable_priority$temperatura_min) {
          if (tmin_var %in% vars) {
            selected_var <- tmin_var
            break
          }
        }
      }
      
      # Si no se encuentra ninguna variable prioritaria, usar la primera disponible
      if (is.null(selected_var) && length(vars) > 0) {
        selected_var <- vars[1]
      }
      
      updateSelectInput(session, "nc_variable",
                        choices = vars,
                        selected = selected_var)
      
      # Mostrar mensaje informativo
      if (!is.null(selected_var)) {
        showNotification(paste("Variable seleccionada automáticamente:", selected_var), 
                         type = "message", duration = 3)
      }
      
    }, error = function(e) {
      showNotification(paste("Error reading NetCDF:", e$message), type = "error")
    })
  })
  
  # === BOTÓN PARA CARGAR NETCDF ===
  output$load_nc_button <- renderUI({
    req(input$nc_file_upload)
    
    tagList(
      selectInput("nc_variable", "Select Variable to Extract:",
                  choices = available_variables(),
                  selected = available_variables()[1]),
      checkboxInput("extract_all_vars", "Extract ALL variables", value = FALSE),
      actionButton("load_nc_data", "Load Data from NetCDF")
    )
  })
  
  # === CARGA DE DATOS NETCDF (MODIFICADA PARA MÚLTIPLES ESTACIONES) ===
  observeEvent(input$load_nc_data, {
    req(input$nc_file_upload)
    
    shinyalert(
      title = "Loading Data from NetCDF", 
      text = "Extracting climate data for all stations...",
      type = "info",
      showCancelButton = FALSE,
      showConfirmButton = FALSE,
      animation = "slide-from-bottom"
    )
    
    # Obtener valores necesarios DENTRO del contexto reactivo
    nc_path <- input$nc_file_upload$datapath
    vars_to_extract <- if(isTRUE(input$extract_all_vars)) {
      NULL
    } else {
      input$nc_variable
    }
    start_year <- input$select_range[1]
    end_year <- input$select_range[2]
    station_source <- current_station_source()
    custom_stations_data <- custom_stations()
    
    # Para estaciones pre-cargadas, obtener la información de la estación seleccionada ANTES del future
    selected_station <- NULL
    if(station_source == "preloaded") {
      selected_station <- selected_station_info()
      if(is.null(selected_station)) {
        shinyalert("Error", "No station selected from pre-loaded stations", type = "error")
        return()
      }
    }
    
    # Mostrar progreso
    showNotification("Processing NetCDF file for all stations...", duration = NULL, id = "nc_progress")
    
    tryCatch({
      # Procesar en segundo plano con future
      future_promise({
        # Determinar qué estaciones procesar usando los valores capturados
        if(station_source == "custom" && !is.null(custom_stations_data)) {
          stations_to_process <- custom_stations_data
          cat("Processing", nrow(stations_to_process), "custom stations\n")
        } else if(station_source == "preloaded" && !is.null(selected_station)) {
          # Usar la estación seleccionada que capturamos antes del future
          stations_to_process <- selected_station
          cat("Processing preloaded station:", selected_station$station_name, "\n")
        } else {
          stop("No stations available to process")
        }
        
        # Extraer datos para todas las estaciones
        extract_multiple_stations(
          nc_path = nc_path,
          stations_data = stations_to_process,
          selected_vars = vars_to_extract,
          start_year = start_year,
          end_year = end_year
        )
      }) %...>% {
        processed_data <- .
        
        # Actualizar reactive values - GUARDAR TODOS LOS DATOS EN multi_nc_data
        multi_nc_data(processed_data)
        
        # Para nc_data, mostrar solo la estación seleccionada (si hay selección)
        if(station_source == "custom" && !is.null(input$custom_station_select)) {
          single_station_data <- processed_data %>% 
            filter(station_name == input$custom_station_select)
          nc_data(single_station_data)
        } else {
          nc_data(processed_data)
        }
        
        # Crear título dinámico
        if(station_source == "custom" && !is.null(custom_stations_data) && nrow(custom_stations_data) > 1) {
          station_count <- length(unique(processed_data$station_name))
          climate_vars <- names(processed_data)[!names(processed_data) %in% 
                                                  c("date", "station_name", "station_id", "climate_id", 
                                                    "lat", "lon", "prov", "grid_lat", "grid_lon", 
                                                    "year", "month", "day")]
          
          nc_data_title(paste0(station_count, " Stations | Variables: ", paste(climate_vars, collapse = ", "), 
                               " | Records: ", nrow(processed_data)))
        } else {
          station_name <- if(nrow(processed_data) > 0) processed_data$station_name[1] else "Unknown"
          climate_vars <- names(processed_data)[!names(processed_data) %in% 
                                                  c("date", "station_name", "station_id", "climate_id", 
                                                    "lat", "lon", "prov", "grid_lat", "grid_lon", 
                                                    "year", "month", "day")]
          
          nc_data_title(paste0(station_name, " | Variables: ", paste(climate_vars, collapse = ", "), 
                               " | Records: ", nrow(processed_data)))
        }
        
        removeNotification("nc_progress")
        shinyjs::runjs("swal.close();")
        
        if(station_source == "custom" && !is.null(custom_stations_data) && nrow(custom_stations_data) > 1) {
          station_count <- length(unique(processed_data$station_name))
          shinyalert("Success!", 
                     paste("Successfully extracted data for", station_count, "stations |", 
                           nrow(processed_data), "total records"),
                     type = "success")
        } else {
          shinyalert("Success!", 
                     paste(nrow(processed_data), "records loaded"),
                     type = "success")
        }
        
      } %...!% {
        # Manejar errores
        removeNotification("nc_progress")
        shinyjs::runjs("swal.close();")
        shinyalert("Error", paste("Cannot load NetCDF:", .$message), type = "error")
      }
      
    }, error = function(e) {
      removeNotification("nc_progress")
      shinyjs::runjs("swal.close();")
      shinyalert("Error", paste("Cannot load NetCDF:", e$message), type = "error")
    })
  })
  
  # ==============================================================================
  # MÓDULO: FORMATO MATRIZ DE ESTACIONES (MODIFICADO - CON OPCIÓN MENSUAL)
  # ==============================================================================
  
  # === FUNCIÓN PARA TRANSFORMAR A FORMATO MATRIZ DIARIO (ORIGINAL) ===
  transform_to_matrix_format <- function(daily_data, target_variable = NULL) {
    if(is.null(daily_data) || nrow(daily_data) == 0) return(NULL)
    
    # Obtener metadatos
    meta_cols <- c("date", "station_name", "station_id", "climate_id", 
                   "lat", "lon", "prov", "grid_lat", "grid_lon", 
                   "year", "month", "day")
    
    # Encontrar variables climáticas
    climate_vars <- names(daily_data)[!names(daily_data) %in% meta_cols]
    
    if(length(climate_vars) == 0) return(NULL)
    
    # Seleccionar variable objetivo
    if(is.null(target_variable)) {
      target_var <- climate_vars[1]
    } else {
      target_var <- target_variable
    }
    
    # Verificar que la variable objetivo existe
    if(!target_var %in% climate_vars) {
      return(NULL)
    }
    
    # Verificar que hay múltiples estaciones
    unique_stations <- unique(daily_data$station_name)
    if(length(unique_stations) <= 1) {
      cat("Only one station found:", paste(unique_stations, collapse = ", "), "\n")
      return(NULL)
    }
    
    cat("Transforming to matrix format for stations:", paste(unique_stations, collapse = ", "), "\n")
    cat("Using variable:", target_var, "\n")
    
    # Transformar a formato wide (matriz) - CORREGIDO
    matrix_format <- daily_data %>%
      select(date, station_name, !!sym(target_var)) %>%
      pivot_wider(
        names_from = station_name,
        values_from = !!sym(target_var)
      ) %>%
      arrange(date)
    
    # Renombrar columnas a v1, v2, v3, etc. manteniendo date como primera columna
    if(ncol(matrix_format) > 1) {
      new_names <- c("date", paste0("v", 1:(ncol(matrix_format)-1)))
      names(matrix_format) <- new_names
    }
    
    cat("Matrix format created with", nrow(matrix_format), "rows and", ncol(matrix_format), "columns\n")
    cat("Column names:", paste(names(matrix_format), collapse = ", "), "\n")
    
    return(matrix_format)
  }
  
  # === FUNCIÓN PARA CREAR MATRIZ MENSUAL (NUEVA) ===
  create_monthly_matrix <- function(daily_data, target_variable = NULL, agg_fun = "sum") {
    if(is.null(daily_data) || nrow(daily_data) == 0) return(NULL)
    
    # Obtener metadatos
    meta_cols <- c("date", "station_name", "station_id", "climate_id", 
                   "lat", "lon", "prov", "grid_lat", "grid_lon", "day")
    
    # Encontrar variables climáticas
    climate_vars <- names(daily_data)[!names(daily_data) %in% meta_cols]
    
    if(length(climate_vars) == 0) return(NULL)
    
    # Seleccionar variable objetivo
    if(is.null(target_variable)) {
      target_var <- climate_vars[1]
    } else {
      target_var <- target_variable
    }
    
    # Verificar que la variable objetivo existe
    if(!target_var %in% climate_vars) {
      return(NULL)
    }
    
    # Obtener estaciones únicas
    unique_stations <- unique(daily_data$station_name)
    if(length(unique_stations) == 0) return(NULL)
    
    cat("Creating monthly matrices for", length(unique_stations), "stations\n")
    
    # Lista para almacenar matrices de cada estación
    station_matrices <- list()
    
    for(station in unique_stations) {
      cat("Processing station:", station, "\n")
      
      # Filtrar datos para esta estación
      station_data <- daily_data %>% filter(station_name == station)
      
      # Agregar a nivel mensual
      monthly_data <- station_data %>%
        mutate(
          year = as.numeric(format(date, "%Y")),
          month = as.numeric(format(date, "%m"))
        ) %>%
        group_by(year, month) %>%
        summarise(
          value = {
            if(agg_fun == "sum") sum(!!sym(target_var), na.rm = TRUE)
            else if(agg_fun == "mean") mean(!!sym(target_var), na.rm = TRUE)
            else if(agg_fun == "max") max(!!sym(target_var), na.rm = TRUE)
            else if(agg_fun == "min") min(!!sym(target_var), na.rm = TRUE)
            else mean(!!sym(target_var), na.rm = TRUE)
          },
          .groups = "drop"
        ) %>%
        # Reemplazar NaN/Inf por NA
        mutate(value = ifelse(is.nan(value) | is.infinite(value), NA, value))
      
      # Crear matriz wide: años como filas, meses como columnas
      station_matrix <- monthly_data %>%
        pivot_wider(
          names_from = month,
          values_from = value,
          names_sort = TRUE
        ) %>%
        arrange(year)
      
      # Renombrar columnas de meses en español
      month_names_spanish <- c("ENE", "FEB", "MAR", "ABR", "MAY", "JUN", 
                               "JUL", "AGO", "SEP", "OCT", "NOV", "DIC")
      
      # Renombrar las columnas numéricas a nombres en español
      for(i in 1:min(12, ncol(station_matrix)-1)) {
        if(as.character(i) %in% names(station_matrix)) {
          names(station_matrix)[names(station_matrix) == as.character(i)] <- month_names_spanish[i]
        }
      }
      
      # Asegurar que todas las columnas de meses existan
      for(month_name in month_names_spanish) {
        if(!month_name %in% names(station_matrix)) {
          station_matrix[[month_name]] <- NA
        }
      }
      
      # Reordenar columnas: YEAR, ENE, FEB, ..., DIC
      station_matrix <- station_matrix %>% 
        select(year, all_of(month_names_spanish))
      
      # Redondear valores a 3 decimales
      station_matrix[month_names_spanish] <- lapply(station_matrix[month_names_spanish], function(x) {
        if(is.numeric(x)) round(x, 3) else x
      })
      
      # Agregar a la lista con el nombre de la estación
      station_matrices[[station]] <- station_matrix
    }
    
    return(station_matrices)
  }
  
  # === ACTUALIZAR DATOS EN FORMATO MATRIZ ===
  observe({
    # Usar multi_nc_data que contiene TODAS las estaciones
    data_to_use <- if(!is.null(multi_nc_data()) && nrow(multi_nc_data()) > 0) {
      multi_nc_data()
    } else {
      NULL
    }
    
    if(!is.null(data_to_use)) {
      if(input$matrix_type == "daily") {
        # Formato diario original
        matrix_formatted <- transform_to_matrix_format(data_to_use, input$matrix_variable)
        if(!is.null(matrix_formatted)) {
          matrix_data(matrix_formatted)
          cat("Daily matrix data updated successfully\n")
        } else {
          cat("Could not create daily matrix format\n")
          matrix_data(tibble())
        }
      } else if(input$matrix_type == "monthly") {
        # Formato mensual nuevo
        matrices <- create_monthly_matrix(
          data_to_use, 
          input$matrix_variable,
          input$monthly_agg_function_matrix
        )
        if(!is.null(matrices)) {
          monthly_matrices(matrices)
          cat("Monthly matrices updated for", length(matrices), "stations\n")
          
          # Actualizar selector de estaciones
          updateSelectInput(session, "selected_station_monthly",
                            choices = names(matrices),
                            selected = if(length(names(matrices)) > 0) names(matrices)[1] else NULL)
        }
      }
    }
  })
  
  # === SELECTOR DE VARIABLE PARA MATRIZ ===
  output$matrix_variable_selector <- renderUI({
    data_to_use <- if(!is.null(multi_nc_data()) && nrow(multi_nc_data()) > 0) {
      multi_nc_data()
    } else {
      NULL
    }
    
    if(!is.null(data_to_use) && nrow(data_to_use) > 0) {
      
      # Obtener variables disponibles
      meta_cols <- c("date", "station_name", "station_id", "climate_id", 
                     "lat", "lon", "prov", "grid_lat", "grid_lon", 
                     "year", "month", "day")
      
      available_vars <- names(data_to_use)[!names(data_to_use) %in% meta_cols]
      
      if(length(available_vars) > 0) {
        selectInput("matrix_variable", "Select Variable for Matrix Display:",
                    choices = available_vars,
                    selected = available_vars[1])
      }
    }
  })
  
  # === DATA TABLE PARA FORMATO MATRIZ ===
  output$matrix_datatable <- DT::renderDataTable({
    if(input$matrix_type == "daily") {
      # Formato diario original
      validate(
        need(nrow(matrix_data()) > 0, 
             "No data available in matrix format. Please upload a CSV with multiple stations and load NetCDF data first.")
      )
      
      spin_matrix$show()
      
      DT::datatable(
        matrix_data(),
        rownames = FALSE,
        extensions = c('Buttons', 'Scroller', 'FixedColumns'),
        options = list(
          dom = 'Bfrtip',
          buttons = list('colvis', 'copy', 'csv', 'excel', 'pdf'),
          scrollX = TRUE,
          deferRender = TRUE,
          scrollY = 600,
          scroller = TRUE,
          pageLength = 20,
          fixedColumns = list(leftColumns = 1)
        ),
        caption = paste("Climate Data Matrix - Stations as Columns (v1, v2, v3...)")
      ) %>%
        DT::formatRound(columns = which(sapply(matrix_data(), is.numeric)), digits = 1)
      
    } else if(input$matrix_type == "monthly") {
      # Formato mensual nuevo
      req(monthly_matrices(), input$selected_station_monthly)
      
      station_matrix <- monthly_matrices()[[input$selected_station_monthly]]
      
      validate(
        need(!is.null(station_matrix), 
             paste("No monthly data available for station:", input$selected_station_monthly))
      )
      
      spin_matrix$show()
      
      DT::datatable(
        station_matrix,
        rownames = FALSE,
        extensions = c('Buttons', 'Scroller'),
        options = list(
          dom = 'Bfrtip',
          buttons = list('copy', 'csv', 'excel'),
          scrollX = TRUE,
          deferRender = TRUE,
          scrollY = 500,
          scroller = TRUE,
          pageLength = 20
        ),
        caption = paste("Monthly Data -", input$selected_station_monthly)
      )
    }
  })
  
  # === BOTÓN DE DESCARGA PARA MATRIZ ===
  output$matrix_download_button <- renderUI({
    if(input$matrix_type == "daily") {
      # Botón para formato diario
      if(nrow(matrix_data()) > 0) {
        tagList(
          downloadButton('downloadMatrixData', 'Download Daily Matrix (.csv)'),
          helpText("Download the data in matrix format with stations as columns")
        )
      }
    } else if(input$matrix_type == "monthly") {
      # Botón para formato mensual
      if(length(monthly_matrices()) > 0) {
        tagList(
          downloadButton('downloadMonthlyMatrix', 'Download All Stations Monthly (.csv)'),
          helpText("Download monthly data for all stations in one CSV file")
        )
      }
    }
  })
  
  # === DESCARGA FORMATO DIARIO (ORIGINAL) ===
  output$downloadMatrixData <- downloadHandler(
    filename = function() {
      paste0("climate_matrix_daily_", 
             ifelse(!is.null(input$matrix_variable), input$matrix_variable, "data"),
             "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
    },
    content = function(file) {
      write.csv(matrix_data(), file, row.names = FALSE)
    }
  )
  
  # === DESCARGA FORMATO MENSUAL (NUEVO) ===
  output$downloadMonthlyMatrix <- downloadHandler(
    filename = function() {
      paste0("climate_matrix_monthly_", 
             ifelse(!is.null(input$matrix_variable), input$matrix_variable, "data"),
             "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
    },
    content = function(file) {
      req(monthly_matrices())
      
      showNotification("Creating monthly data file for all stations...", type = "message")
      
      # Combinar todas las matrices en un solo data frame
      all_data <- list()
      
      for(station_name in names(monthly_matrices())) {
        station_data <- monthly_matrices()[[station_name]] %>%
          mutate(Station = station_name) %>%
          select(Station, everything())
        
        all_data[[station_name]] <- station_data
      }
      
      # Combinar todos los datos
      combined_data <- bind_rows(all_data)
      
      # Escribir CSV
      write.csv(combined_data, file, row.names = FALSE, na = "")
      
      showNotification("Monthly data file created successfully!", type = "message")
    }
  )
  
  # ==============================================================================
  # MÓDULO: VISUALIZACIONES Y SALIDAS
  # ==============================================================================
  
  # === MAPA DE ESTACIONES ===
  output$MapPlot <- renderLeaflet({
    req(all_stations_data())
    
    # Datos de estaciones
    map_data <- all_stations_data() %>%
      dplyr::filter(!is.na(lon), !is.na(lat)) %>%
      dplyr::mutate(
        text = paste(sep = "<br/>", 
                     paste("<b>", station_name, "</b>"),
                     paste0("Source: ", source),
                     paste0("Location: ", round(lat, 4), "°, ", round(lon, 4), "°"),
                     ifelse(!is.na(elev) & elev != "", paste0("Elevation: ", elev, "m"), ""),
                     ifelse(source == "preloaded", 
                            paste0("Climate ID: ", climate_id), 
                            "Custom Station")
        )
      )
    
    # Obtener la extensión del NetCDF si existe
    bbox <- nc_bbox()
    
    if(nrow(map_data) == 0 && is.null(bbox)) {
      return(leaflet::leaflet() %>% 
               leaflet::addTiles() %>% 
               leaflet::setView(lng = -75.0, lat = -9.0, zoom = 5) %>%
               leaflet::addPopups(lng = -75.0, lat = -9.0, "No valid station coordinates available"))
    }
    
    pal <- leaflet::colorFactor(
      palette = c("blue", "red"),
      domain = map_data$source
    )
    
    # Crear mapa base
    map <- leaflet::leaflet(map_data) %>%
      leaflet::addTiles(urlTemplate = 'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png') %>%
      leaflet::setView(lng = -75.0, lat = -9.0, zoom = 5)
    
    # Agregar estaciones si existen
    if(nrow(map_data) > 0) {
      map <- map %>%
        leaflet::addCircleMarkers(
          lng = ~lon, 
          lat = ~lat, 
          popup = ~text,
          color = ~pal(source),
          stroke = FALSE,
          fillOpacity = 0.8,
          radius = 8,
          clusterOptions = leaflet::markerClusterOptions()
        ) %>%
        leaflet::addLegend(
          "bottomright",
          pal = pal,
          values = ~source,
          title = "Station Source",
          opacity = 1
        )
    }
    
    # Agregar rectángulo del NetCDF si existe
    if(!is.null(bbox)) {
      map <- map %>%
        leaflet::addRectangles(
          lng1 = bbox$west, lat1 = bbox$south,
          lng2 = bbox$east, lat2 = bbox$north,
          popup = paste(
            "<b>NetCDF Coverage</b><br>",
            "North: ", round(bbox$north, 2), "°<br>",
            "South: ", round(bbox$south, 2), "°<br>",
            "East: ", round(bbox$east, 2), "°<br>",
            "West: ", round(bbox$west, 2), "°<br>",
            "Variables: ", length(available_variables())
          ),
          fillColor = "#1E90FF",
          fillOpacity = 0.3,
          color = "#FF6B00",
          weight = 3,
          opacity = 0.8,
          group = "NetCDF Coverage"
        )
    }
    
    # Agregar controles de capas si hay ambos tipos de datos
    if(nrow(map_data) > 0 && !is.null(bbox)) {
      map <- map %>%
        leaflet::addLayersControl(
          overlayGroups = c("Stations", "NetCDF Coverage"),
          options = layersControlOptions(collapsed = FALSE)
        )
    }
    
    # Si hay NetCDF, ajustar la vista para mostrar todo
    if(!is.null(bbox)) {
      map <- map %>%
        leaflet::fitBounds(
          lng1 = bbox$west, lat1 = bbox$south,
          lng2 = bbox$east, lat2 = bbox$north
        )
    }
    
    return(map)
  })
  
  # === TÍTULOS DE DATOS ===
  output$data_preview_title_plot <- output$data_preview_title <- renderUI({
    if(nrow(nc_data()) == 0) {
      HTML("<b>Currently displayed data: </b><br>No climate data loaded. Please upload NetCDF file and click 'Load Data from NetCDF'")
    } else if(current_station_source() == "custom") {
      HTML("<b>Currently displayed data: </b><br>Custom Stations loaded - Please use 'Station Matrix' tab for data visualization")
    } else {
      HTML(paste0("<b>Currently displayed data: </b><br>", nc_data_title()))
    }
  })
  
  # ReadMe Tab
  output$README <- renderUI({
    if(file.exists("D:/Python_R/R/App/Data/README.md")) {
      includeMarkdown("D:/Python_R/R/App/Data/README.md")
    } else {
      includeMarkdown("D:/Python_R/R/App/Data/README.md")
    }
  })
  
  # Station info date
  current_rds_date <- reactivePoll(1000, session,
                                   checkFunc = function() {
                                     file.info("D:/Python_R/R/App/Data/stations_peru.rds")$mtime[1] 
                                   },
                                   valueFunc = function() {
                                     file.info("D:/Python_R/R/App/Data/stations_peru.rds")$mtime[1] %>% 
                                       base::as.Date(tz = "America/Lima")
                                   }
  )
  
  output$info_date <- renderText({
    paste("Station data last updated:", current_rds_date() %>% as.character())
  })
  
  # === DATA TABLE OPTIMIZADO ===
  output$datatable <- DT::renderDataTable({
    # Verificar si hay datos y el tipo de estación
    if(nrow(nc_data()) == 0) {
      return(DT::datatable(
        data.frame(Message = "No climate data available. Please load NetCDF data first."),
        options = list(dom = 't', pageLength = 5),
        caption = "Data Status"
      ))
    }
    
    # Verificar si son custom stations
    if(current_station_source() == "custom") {
      return(DT::datatable(
        data.frame(Information = "Raw Data Table is designed for Pre-loaded Stations. For Custom Stations, please use the 'Station Matrix' tab to view and download data in matrix format."),
        options = list(dom = 't', pageLength = 5),
        caption = "Feature Information"
      ))
    }
    
    spin_datatable$show()
    
    # Mostrar solo las primeras columnas para optimizar rendimiento
    data_to_show <- nc_data()
    if(ncol(data_to_show) > 15) {
      data_to_show <- data_to_show[, 1:15]  # Mostrar solo primeras 15 columnas
    }
    
    DT::datatable(
      data_to_show,
      rownames = FALSE,
      extensions = c('Buttons', 'Scroller'),
      options = list(
        dom = 'Bfrtip',
        buttons = list('colvis', 'copy', 'csv', 'excel', 'pdf'),
        scrollX = TRUE,
        deferRender = TRUE,
        scrollY = 600,
        scroller = TRUE,
        pageLength = 20
      )
    )
  })
  
  # === EXPLORADOR DE DATOS FALTANTES ===
  output$pctmiss_plotly <- renderPlotly({
    validate(need(nrow(nc_data()) > 0, "No data available. Please load NetCDF data first."))
    
    spin_plot$show()
    on.exit({ spin_plot$hide() })
    
    data_to_plot <- nc_data()
    
    climate_vars <- data_to_plot %>% 
      select(-date, -station_name, -station_id, -climate_id, -lat, -lon, -prov, -grid_lat, -grid_lon)
    
    if(ncol(climate_vars) > 6) {
      main_vars <- c("year")
      priority_vars <- c("pr", "tas", "tasmax", "tasmin", "pcp", "temperature")
      available_priority <- intersect(priority_vars, names(climate_vars))
      
      if(length(available_priority) > 0) {
        VAR_COLS <- climate_vars %>% select(year, all_of(available_priority[1:min(4, length(available_priority))]))
      } else {
        VAR_COLS <- climate_vars %>% select(1:5)
      }
    } else {
      VAR_COLS <- climate_vars
    }
    
    TICK_FULL <- unique(VAR_COLS$year)
    
    if(length(TICK_FULL) > 80){
      TICK_REDUCED <- ifelse(as.numeric(TICK_FULL) %% 10 == 0, TICK_FULL, "")
    } else if (length(TICK_FULL) < 20){
      TICK_REDUCED <- TICK_FULL
    } else {
      TICK_REDUCED <- ifelse(as.numeric(TICK_FULL) %% 5 == 0, TICK_FULL, "")
    }
    
    miss_plot <- gg_miss_fct(VAR_COLS, year) + 
      scale_x_discrete(limits = TICK_FULL, breaks = TICK_FULL, labels = TICK_REDUCED) +
      labs(title = "Missing Climate Data by Year", x = "Year", y = "Climate Variables")
    
    ggplotly(miss_plot)
  })
  
  # ==============================================================================
  # FINALIZACIÓN DE LA APLICACIÓN
  # ==============================================================================
  
  # End the app loading spinner
  waiter_hide()
}

# ==============================================================================
# EJECUCIÓN DE LA APLICACIÓN
# ==============================================================================

shinyApp(ui = ui, server = server)