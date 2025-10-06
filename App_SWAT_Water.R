library(shiny)
library(mapedit)
library(leaflet)
library(shinyjs)
library(sf)
library(shinyWidgets)
library(blastula)
library(RPostgres)
library(dplyr)
library(DBI)
library(shinythemes)


# load separate module and function scripts
source("D:/Paper_Climate/Data/siguiente paper/App/App_SWAT/mod_request_weather.R")
source("D:/Paper_Climate/Data/siguiente paper/App/App_SWAT/mod_upload_shape.R")
source("D:/Paper_Climate/Data/siguiente paper/App/App_SWAT/mod_track_requests.R")
source("D:/Paper_Climate/Data/siguiente paper/App/App_SWAT/fct_helpers.R")

# Define User Interface 
# List the first level UI elements here 
app_ui <- bootstrapPage(
  tags$head(includeCSS("D:/Paper_Climate/Data/siguiente paper/App/App_SWAT/custom.css")),
  #add_busy_spinner(spin = "orbit", color = "#DCDCDC", position = "full-page"),
  tags$head(tags$style(".leaflet-top {z-index:999!important;}")),
  navbarPage(
    theme = shinytheme('sandstone'),
    collapsible = TRUE,
    id = "weather_app",
    "W3S-Water",
    
    # UI for Weather Time-series Download
    tabPanel(
      "Weather Data",
      mod_request_weather_ui("request_weather_ui_1")
    ),
    
    # UI for SWAT Weather Statistics
    # tabPanel(
    #   "Weather Statistics",
    #   mod_weather_stats_ui("weather_stats_ui_1")
    # ),
    
    # UI for SWAT Weather Statistics Download
    tabPanel(
      "Track Requests",
      mod_track_requests_ui("track_requests_ui_1")
    ),
    
    # UI for About tab (regarding data, framework and citations)
    tabPanel(
      "About",
      icon = icon("info"),
      tags$div(
        tags$h3("About this Service"),
        "This application, also called the World Weather for Water Data Service (W3S), is a Data as a Service (DaaS) platform that allows
             application users to download climate data of a pre-specified region within any watershed across the globe. The weather data can be downloaded in 
             multiple hydrologic model compatible formats including SWAT, HEC-HMS and generic CSV format for compatibility with other hydrologic models, including
             GR4J and Machine Learning based rainfall runoff models. W3S can thus be considered a global weather data playgoround designed to swiftly assist users
             with developing both conceptual and data-driven hydrologic and rainfall-runoff models.",tags$br(),
        
        tags$h3("Data Sources"),
        tags$b("Historical Data:"), "TBA", tags$br(), tags$br(),
        
        tags$h3("Code"),
        "This application is developed using R Shiny. The application code is available on TBA ", tags$br(),
        
        tags$h3("Authors"),
        "Mr. Uttam Ghimire, Watershed Research Group, School of Engineering, University of Guelph, ON, Canada and Stockholm Environment Institute Asia Center,
             Bangkok, Thailand",tags$br(),
        "Dr Taimoor Akhtar, Watershed Research Group, School of Engineering, University of Guelph, ON, Canada",tags$br(),
        "Dr Narayan K. Shrestha, TBA",tags$br(),
        "Dr Prasad Daggupati, Watershed Research Group, School of Engineering, University of Guelph, ON, Canada", tags$br(),
        
        tags$h3("Contact"),
        "akhtart@uoguelph.ca",tags$br(),tags$br()
        
      )
      
    ),
    
    # UI for Help tab (regarding navigation)
    tabPanel(
      "Help",
      icon = icon("question"),
      
      tags$div(
        tags$h3("Weather Data Request"),
        "In order to download historical weather data, first choose a  geographical area by either i) Using the polygon drawing tool 
              (left side of map) to draw a custom area or ii) uploading a zipped boundary (single polygon) shape file,
              using the 'DATA SELECTOR' panel. Data may be filtered further by selecting climate parameter and date 
              ranges in the 'DATA SELECTOR' before submission of request. Once a valid data requet is selected, users will be 
              required to provide and email address for further processing of their data request. Please note that there may be a wait time of a
              few hours or days before data is processed and is available for download.",tags$br(),
        
        tags$h3("Shape File Format"),
        "If preferred area selection mechanism is a user-provided ", tags$i("Shape File"), "then please make sure that all files associated with
             the uploaded shape input (e.g., *.shp, *.dbf etc) are in a zipped folder. Also the shape should be a boundary / single polygon.", tags$br(),
        
        tags$h3("Track Data Request"),
        "Users will be intimated via email once a data request is processed. They can then download the requested data from the Track Requests tab. 
            Users will be required to provide their email address to be able to download the requested data. Users can also track progress of all pending data
             requests from the Track Requests tab.", tags$br(),
        
        tags$h3("Contact Information"),
        "Please contact us at: ", tags$b("pdaggupa@uoguelph.ca"), "for further queries and information.", tags$br(), tags$br(), tags$br()
      )
      
    )
  )
)

# Define Server function
app_server <- function( input, output, session ) {
  # List the first level callModules here
  
  callModule(mod_request_weather_server, "request_weather_ui_1")
  
  # callModule(mod_weather_stats_server, "weather_stats_ui_1")
  
  callModule(mod_track_requests_server, "track_requests_ui_1")
  
}


# Run the application 
shinyApp(ui = app_ui, server = app_server)
