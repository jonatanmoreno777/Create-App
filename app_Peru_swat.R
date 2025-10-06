library(shiny)
library(shinythemes)
library(shinyjs)
library(shinyBS)
library(leaflet)
library(leaflet.extras)
library(rgdal)
library(sp)
library(sf)
library(geoshaper)
library(mapview)


# Get data of historical and climate change data grid points -------------
data <- st_read('D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/Climate change/RAIN4PE10KM.shp')
st_geometry(data) <- NULL
cc_points <- SpatialPointsDataFrame(data[,c('long', 'Lat')] , data)
cc_points$secondLocationID <- paste(as.character(cc_points$SN), "_selectedLayer", sep="")

data <- st_read('D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/Historical Observed/PISCO10KMSWAT.shp') # primero se tiene .txt luego
st_geometry(data) <- NULL                                                                                      # se exporta a qgis (.shp)
hist_points <- SpatialPointsDataFrame(data[,c('X_coord', 'Y_coord')] , data)
hist_points$secondLocationID <- paste(as.character(hist_points$SN), "_selectedLayer", sep="")


# Create Basemap ---------------------------------------------------------
boundary <- readOGR(dsn='D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/basin/', layer='PeruHydrographicSlope')
#boundary <- readOGR(dsn='D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/basin', layer='CanadianGreatLakes')
# Crear etiquetas para los polígonos
pal <- colorBin("YlOrRd", domain = boundary$AREA)
labels <- sprintf(
  "<strong>Lake %s</strong><br/> Area: %f th. mi<sup>2</sup>",
  boundary$HIDROGRAPH, boundary$AREA
) %>% lapply(htmltools::HTML)  #%>% unlist()

# Definir los colores de los polígonos
boundary$col <- c('red', 'blue', 'green')

# Crear el mapa base con Leaflet
basemap = leaflet(boundary, options = leafletOptions(minZoom = 5)) %>%
    addTiles(
      urlTemplate = "//{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
      attribution = 'Maps by <a href="http://www.openstreetmap.org">OpenStreetMap</a>'
  ) %>%
  # setView(-80, lat = 45, zoom=6) %>%
  # setMaxBounds(lng1 = -95
  #              , lat1 = 35
  #              , lng2 = -65
  #              , lat2 = 55 ) %>%
  #addMouseCoordinates() %>% 
  addDrawToolbar(
    targetGroup='Selected',
    polylineOptions=FALSE,
    markerOptions = FALSE,
    polygonOptions = drawPolygonOptions(shapeOptions=drawShapeOptions(fillOpacity = 0
                                                                      ,color = 'red'
                                                                      ,weight = 3)),
    rectangleOptions = drawRectangleOptions(shapeOptions=drawShapeOptions(fillOpacity = 0
                                                                          ,color = 'red'
                                                                          ,weight = 3)),
    circleOptions = FALSE,
    circleMarkerOptions = FALSE,
    singleFeature = TRUE,
    editOptions = editToolbarOptions(edit = FALSE, remove = TRUE, selectedPathOptions = selectedPathOptions())) %>%
  addPolygons(color = "black", dashArray = "3", fillColor = boundary$col, weight = 2, smoothFactor = 0.5,
              opacity = 0.2, highlight = highlightOptions(weight = 5,
                                                          color="red",
                                                          fillOpacity = 0.4,
                                                          dashArray = "",
                                                          bringToFront = FALSE),
              label = labels, stroke = FALSE)
# Mostrar el mapa
#basemap

# Load Statistics Data
get_climate_statistics <- function(){
  withProgress(message = 'Loading Data', value=0,{
    data <- read.csv(file="D:/Paper_Climate/Data/siguiente paper/App/app2/GL_SWAT_all_WGN.csv", header=TRUE, sep=",")
    data$secondLocationID <- paste(as.character(data$OBJECTID), "_selectedLayer", sep="")
    cdata <- SpatialPointsDataFrame(data[,c('WLONGITUDE', 'WLATITUDE')] , data)
  })
} 


# Create user interface -------------------------------
ui <- navbarPage(
  theme = shinytheme('flatly'),
  #shinythemes::themeSelector(),  # <--- Add this somewhere in the UI,
  #theme = "flatlyST_bootstrap_CTedit.css",
  inverse = F, 
  id = "masters_golf",
  # App Title
  "Peru-Weather",
  #tags$div(tags$img(src='masters_logo_3.png', width = 108, height = 108, style="float:left; margin-left: 5px; margin-right: 5px; margin-top: -15px")), 
  
  # Historical Data Selector and Downloader -------------------------------
  tabPanel("Pisco Data",
           shinyjs::useShinyjs(),
           div(class="outer",
               tags$head(includeCSS("D:/Paper_Climate/Data/siguiente paper/App/app2//styles.css")),
               tags$style(
                 HTML("
                         .Relative Humidity-item {
                             margin-bottom: 15px; /* Ajusta el espacio entre los parámetros */
                         }
                     ")
               ),
               shinyjs::useShinyjs(),
               leafletOutput("hist_map", width="100%", height="100%"),
               absolutePanel(id = "controls", class = "panel panel-default", fixed = TRUE,
                             draggable = TRUE, top = 90, left = "auto", right = 20, bottom = "auto",
                             width = 350, height = "auto",
                             
                             h2("Data Selector"),
                             
                             
                             checkboxGroupInput("hist_dataGroup", label = h4("Parameters:"), 
                                                choices = list("Precipitation (mm)" = 1, "Temperature (C)" = 2),
                                                selected = 1, inline = TRUE),
                             
                             dateRangeInput('hist_dateRange',
                                            label = h4('Date Range:'),
                                            start = as.Date("1981-01-01"), end = as.Date("2016-12-31"),
                                            min = as.Date("1981-01-01"), max = as.Date("2016-12-31")),
                             
                             radioButtons("hist_radio", label = "Upload Shape File?",
                                          choices = c("Yes" = 1, "No" = 2), 
                                          inline=TRUE, selected = 2),
                             #bsTooltip("hist_radio", "Please make sure that shape file contains a single polygon and is zipped."),
                             conditionalPanel(condition = "input.hist_radio == 1",
                                              # Input: Select a file ----
                                              fileInput("hist_shpFile", "Upload Zipped Boundary Shape File", accept = ".zip",
                                                        multiple = FALSE)),
                             
                             disabled(downloadButton("hist_downloadClimate", "Download Data"))),
               
               absolutePanel(id = "logo", class = "card", bottom = 25, right = 30, width = 110, fixed=TRUE, draggable = FALSE, height = "auto",
                             tags$a(href='https://www.uoguelph.ca', tags$img(src='https://es.m.wikipedia.org/wiki/Archivo:Escudo_de_la_UNSCH.png',height='40',width='120'))),
               
               absolutePanel(id = "logo", class = "card", bottom = 25, right = 150, width = 30, fixed=TRUE, draggable = FALSE, height = "auto",
                             actionButton("twitter_share", label = "", icon = icon("twitter"),style='padding:5px',
                                          onclick = sprintf("window.open('%s')", 
                                                            "https://twitter.com/intent/tweet?text=%20@UOG_Watershed%20Can-GLWS%20SWAT%20weather%20data%20service&url=https://www.uoguelph.ca/watershed/glws/&hashtags=GreatLakes")))
               
           )),
  
  tabPanel("Rain4pe Data",
           shinyjs::useShinyjs(),
           div(class="outer",
               tags$head(includeCSS("D:/Paper_Climate/Data/siguiente paper/App/app2//styles.css")),
               shinyjs::useShinyjs(),
               leafletOutput("map", width="100%", height="100%"),
               absolutePanel(id = "controls", class = "panel panel-default", fixed = TRUE,
                             draggable = TRUE, top = 90, left = "auto", right = 20, bottom = "auto",
                             width = 350, height = "auto",
                             
                             h2("Data Selector"),
                             
                             checkboxGroupInput("dataGroup", label = h4("Parameters:"), 
                                                choices = list("Precipitation (mm)" = 1, "Temperature (C)" = 2,"Wind (m/s)" = 3, 
                                                               "Relative Humidity (%)" = 4, "Solar (MJ/m2)" = 5),
                                                selected = 1, inline = TRUE),
                             
                             dateRangeInput('dateRange',
                                            label = h4('Date Range:'),
                                            start = as.Date("1981-01-01"), end = as.Date("2016-12-31"),
                                            min = as.Date("1981-01-01"), max = as.Date("2016-12-31")),
                             
                             radioButtons("radio", label = "Upload Shape File?",
                                          choices = c("Yes" = 1, "No" = 2), 
                                          inline=TRUE, selected = 2),
                             
                             #bsTooltip("hist_radio", "Please make sure that shape file contains a single polygon and is zipped."),
                             conditionalPanel(condition = "input.radio == 1",
                                              # Input: Select a file ----
                                              fileInput("shpFile", "Upload Zipped Boundary Shape File", accept = ".zip",
                                                        multiple = FALSE)),
                             
                             disabled(downloadButton("downloadClimate", "Download Data"))),
               
               absolutePanel(id = "logo", class = "card", bottom = 25, right = 30, width = 110, fixed=TRUE, draggable = FALSE, height = "auto",
                             tags$a(href='https://www.uoguelph.ca', tags$img(src='D:/Paper_Climate/Data/siguiente paper/App/app2/uog_black.png',height='40',width='120'))),
               
               absolutePanel(id = "logo", class = "card", bottom = 25, right = 150, width = 30, fixed=TRUE, draggable = FALSE, height = "auto",
                             actionButton("twitter_share", label = "", icon = icon("twitter"),style='padding:5px',
                                          onclick = sprintf("window.open('%s')", 
                                                            "https://twitter.com/intent/tweet?text=%20@UOG_Watershed%20Can-GLWS%20SWAT%20weather%20data%20service&url=https://www.uoguelph.ca/watershed/glws/&hashtags=GreatLakes")))
               
           )),
  
  
  # Statistics Data Selector and Downloader -------------------------------
  tabPanel("Weather Statistics",
           fluidRow(
             div(class="outer",
                 tags$head(includeCSS("D:/Paper_Climate/Data/siguiente paper/App/app2//styles.css")),
                 #tags$style(type = "text/css", "#map {height:
                 leafletOutput("stats_map", width="100%", height="100%"),
                 absolutePanel(id = "controls", class = "panel panel-default", fixed = TRUE,
                               draggable = TRUE, top = 90, left = "auto", right = 20, bottom = "auto",
                               width = 400, height = "auto",
                               
                               h2("Data Selector"),
                               
                               radioButtons("radio_stats", label = h4("Upload Shape File?"),
                                            choices = c("Yes" = 1, "No" = 2),
                                            inline=TRUE, selected = 2),
                               bsTooltip("radio_stats", "Download options will be enabled once a region is selected and data is processed."),
                               conditionalPanel(condition = "input.radio_stats == 1",
                                                # Input: Select a file ----
                                                fileInput("stats_shpFile", "Upload Zipped Boundary Shape File", accept = ".zip",
                                                          multiple = FALSE)),
                               #bsTooltip("stats_shpFile", "Please make sure that shape file contains a single polygon and is zipped."),
                               disabled(downloadButton("stats_download", "Download Data"))),
                 
                 absolutePanel(id = "logo", class = "card", bottom = 25, right = 30, width = 110, fixed=TRUE, draggable = FALSE, height = "auto",
                               tags$a(href='https://www.uoguelph.ca', tags$img(src='D:/Paper_Climate/Data/siguiente paper/App/app2/images.png',height='40',width='120'))),
                 
                 absolutePanel(id = "logo", class = "card", bottom = 25, right = 150, width = 30, fixed=TRUE, draggable = FALSE, height = "auto",
                               actionButton("twitter_share", label = "", icon = icon("twitter"),style='padding:5px',
                                            onclick = sprintf("window.open('%s')", 
                                                              "https://twitter.com/intent/tweet?text=%20@UOG_Watershed%20Can-GLWS%20SWAT%20weather%20data%20service&url=https://www.uoguelph.ca/watershed/glws/&hashtags=GreatLakes")))
                 
             ))),
  
  # About --------------------------------------------------------
  tabPanel("About", icon = icon("info"),
           tags$div(
             tags$h4("About this Service"),
             "This application, also called the Great Lakes Weather Data Service for SWAT (Can-GLWS), is a Data as a Service (DaaS) platform that allows
             application users to download SWAT-ready climate data (historical, climate change scenarios and weather statistics) of a pre-specified region
             within the Canadian Great Lakes watersheds. The aim of this application is to remove the redundancy associated with SWAT-model weather data
             preparation. Service / app users simply need to provide / delineate their area of interest (within the Canadian Great Lakes watersheds), 
             and they can subsequently download their desired SWAT-model-ready weather inputs (historical, climate change-related or weather statistics).",tags$br(),
             
             tags$h4("Data Sources"),
             tags$b("Historical Data:"), "SWAT-ready historical weather data is derived from the weather dataset provided by Natural Resources Canada (NRCan)
             in the ESRI ASCII file format. The original gridded dataset is available at 300 arc seconds spatial resolution. Further details on the original dataset
             are available in ", tags$a(href="https://journals.ametsoc.org/bams/article/92/12/1611/59964/Customized-Spatial-Climate-Models-for-North", "The Bulletin
                                        of the American Meteorological Society (BAMS)."), tags$br(), tags$br(),
             
             tags$b("Climate Change Data:"), "SWAT-ready climate change data is derived from the PCIC dataset which is archived at Pacific Climate Impact
             Consortium (PCIC)'s ",tags$a(ref="https://data.pacificclimate.org/platform/downscaled_gcms_archive/map/", "website"), ". The  modified PCIC dataset available here includes
             statistically downscaled precipitation, and maximum and minimum temperature data, in SWAT-ready format, at 300 arc seconds (~10km) spatial resolution
             for a simulated period until 2100, including a historical period (1950-2005) which was used to calibrate two downscaling methods. The dataset
             available in this service includes 12 Global Climate Models (GCM), 3 Scenarios and 2 downsclaing methods.", tags$br(), tags$br(),
             
             tags$b("Weather Statistics Database:"), "The SWAT weather generator statistics dataset (pertaining to precipitation and temperature) available in this
             application is calculated using historical data of 66-year period (1950-2015). The historical dataset is available in ",
             tags$a(href="https://journals.ametsoc.org/bams/article/92/12/1611/59964/Customized-Spatial-Climate-Models-for-North", "The Bulletin
                    of the American Meteorological Society (BAMS)."), tags$br(),
             
             tags$h4("Code"),
             "This application is developed using R Shiny. The application code is available on ",
             tags$a(href = "https://github.com/drkupi/APWS", "Github"), tags$br(),
             
             tags$h4("Authors"),
             "Dr Narayan K. Shrestha, Watershed Research Group, School of Engineering, University of Guelph, ON, Canada",tags$br(),
             "Dr Taimoor Akhtar, Watershed Research Group, School of Engineering, University of Guelph, ON, Canada",tags$br(),
             "Uttam Ghimire, Stockholm Environment Institute Asia Center, Bangkok, Thailand",tags$br(),
             "Dr Prasad Daggupati, Watershed Research Group, School of Engineering, University of Guelph, ON, Canada",tags$br(),
             "Dr Ramesh P. Rudra, Watershed Research Group, School of Engineering, University of Guelph, ON, Canada",tags$br(),
             "Dr Pradeep K. Goel, Ministry of the Environment, Conservation and Parks, Etobicoke, ON, Canada",tags$br(),
             "Dr Rituraj Shukla, Watershed Research Group, School of Engineering, University of Guelph, ON, Canada",tags$br(),
             
             tags$h4("Contact"),
             "akhtart@uoguelph.ca",tags$br(),tags$br(),
             
             tags$img(src='D:/Paper_Climate/Data/siguiente paper/App/app2/uog_black.png',height='40',width='120')
             
             
           )
  ),
  
  # Help Tab ------------------------------------------------
  tabPanel("Help", icon = icon("question"),
           
           tags$div(
             tags$h4("Climate Change Data Download"),
             "In order to download climate change data, follow the same workflow as for historical data and first choose a geographical area
              by either i) Using the polygon drawing tool to draw a custom area or ii) uploading a zipped boundary (single polygon) shape file,
              using the 'DATA SELECTOR' panel. Once a valid area is selected that has atleast one weather data point, the download button will
              be enabled. Data may be filtered furtherby changing the Climate Model, Scenario etc., and/or by selecting climate parameter and date 
              ranges in the 'DATA SELECTOR' before final download. Please note that there may be a wat time of a
              few seconds or minutes after the download button is clicked and before download begins (since the data is being prepared).",tags$br(),
             
             tags$h4("Weather Statistics Data Download"),
             "In order to download weather statistics data (in SWAT format) choose a geographical area by either i) Using the polygon drawing tool
              to draw a custom area or ii) uploading a zipped boundary (single polygon) shape file, using the 'DATA SELECTOR' panel.
              Once a valid area is selected that has atleast one weather data point, the download button will be enabled.Please note that there may be a wat time of a
              few seconds or minutes after the download button is clicked and before download begins (since the data is being prepared).",tags$br(),
             
             tags$h4("Shape File Format"),
             "If preferred area selection mechanism is a user-provided ", tags$i("Shape File"), "then please make sure that all files associated with
             the uploaded shape input (e.g., *.shp, *.dbf etc) are in a zipped folder. Also the shape should be a boundary / single polygon.", tags$br(),
             
             tags$h4("Contact Information"),
             "Please contact us at: ", tags$b("pdaggupa@uoguelph.ca"), "for further queries and information.", tags$br(), tags$br(), tags$br(),
             
             tags$img(src='uog_black.png',height='40',width='120')
           )
  ),
  windowTitle = "Great Lakes Climate Data"
)

###############################################################################
#                            input, output
###############################################################################

server <- function(input, output, session) {
  
  error_id <<- NULL
  selected <<- NULL
  hist_selected <<- NULL
  stats_all <<- NULL
  sdata <<- NULL
  

  ## UI END for Climate Data Selection --------------
  
  #########################################################################
  ## UI response start for for historical Data ADDITIONAL Selection --------------
  ########################################################################
  
  output$map <- renderLeaflet({
    basemap
  })
  
  observeEvent(input$radio,{
    choice <- input$radio
    if (choice == 1){
      proxy <- leafletProxy("map")
      proxy %>% clearMarkers()
    }
    
    else{
      proxy <- leafletProxy("map")
      proxy %>% clearMarkers()
    }
  })
  
  # Selection of custom drawn map 
  observeEvent(input$map_draw_new_feature,{
    
    # Step 1 - Filter data points within region
    
    found_in_bounds <- findLocations(shape = input$map_draw_new_feature
                                     , location_coordinates = cc_points
                                     , location_id_colname = "SN")
    selected <<- subset(cc_points, SN %in% found_in_bounds)
    
    # Step 3 - Print Data Summary of Selected Data and Paste their Locations    
    # output$selected_var <- renderText({ 
    #   paste("Number of Observations Found: ",length(selected))
    # })
    proxy <- leafletProxy("map")
    proxy %>% clearMarkers()
    #proxy %>% setView(lng=viewCenter[1], lat = viewCenter[2], input$Map_zoom)
    proxy %>% addCircleMarkers(data = selected,
                               radius = 3,
                               lat = selected$Lat,
                               lng = selected$long,
                               fillOpacity = 0.5,
                               color = "blue",
                               stroke = FALSE,
                               layerId = ~SN
    )
    # Step 4 - Save Data as csv and Enable Download
    if (length(selected) > 0){
      if (is.null(input$dataGroup)){
        shinyjs::disable("downloadClimate")
      }
      else {shinyjs::enable("downloadClimate")}
    }
  })
  
  # The following code handles the case where shape file is provided by user
  observeEvent(input$shpFile, {
    
    if (!is.null(error_id))
      removeNotification(error_id)
    error_id <<- NULL
    
    inFile <- input$shpFile
    if (is.null(inFile))
      return(NULL)
    
    # Once the filepath is known, copy file to server folder
    withProgress(message = "Uploading and unzipping shape file - Please Wait", value=0,{
      #file.copy(inFile$datapath, file.path(getwd(), inFile$name))
      unlink("./Scratch", recursive = TRUE)
      dir.create("Scratch")
      unzip(inFile$datapath, exdir = "./Scratch")
    })
    
    # Check shape file format and load shape file
    dbfFiles <- list.files(path="./Scratch", pattern = "\\.dbf$")
    if (length(dbfFiles) == 0){
      error_id <<- showNotification("No Shape File Found !!!!", type = "error")
    }
    else if (length(dbfFiles) > 1){
      error_id <<- showNotification("Multiple Shape Files found. Choosing first file.", type = "warning")
      withProgress(message = 'Loading Shapes - Please Wait', value=0,{
        customShape <- readOGR(dsn="./Scratch", layer=tools::file_path_sans_ext(dbfFiles[1]))
        customShape <- spTransform(customShape,CRS("+proj=longlat +datum=WGS84"))
      })
    }
    else {
      withProgress(message = 'Loading Shapes - Please Wait', value=0,{
        customShape <- readOGR(dsn="./Scratch", layer=tools::file_path_sans_ext(dbfFiles[1]))
        customShape <- spTransform(customShape,CRS("+proj=longlat +datum=WGS84"))
      })
    }
    
    # Display shapefile on map
    if (length(customShape) > 1){ customShape <- customShape[1]}
    
    newP = customShape@polygons
    selected_locs <- sp::over(cc_points, sp::SpatialPolygons(newP))
    x = (cc_points[which(!is.na(selected_locs)), "SN"])
    selected_loc_id = as.character(x[["SN"]])
    selected <<- subset(cc_points, SN %in% selected_loc_id)
    
    bounds <- sp::bbox(customShape)
    proxy <- leafletProxy("map")
    proxy %>% clearMarkers()
    proxy %>% addPolygons(data=customShape, color = "red", fillColor = "transparent", weight = 4.0)
    proxy %>% addCircleMarkers(data = selected,
                               radius = 3,
                               lat = selected$Lat,
                               lng = selected$long,
                               fillOpacity = 0.5,
                               color = "blue",
                               stroke = FALSE,
                               layerId = ~SN
    )
    proxy %>% fitBounds(lng1 = bounds[1,1], lat1 = bounds[2,1], lng2 = bounds[1,2], lat2 = bounds[2,2])
    
    if (length(selected) > 0){
      if (is.null(input$dataGroup)){
        shinyjs::disable("downloadClimate")
      }
      else {shinyjs::enable("downloadClimate")}
    }
  })
  #########################
  # Downloadable selected Historical data ----
  output$downloadClimate <- downloadHandler(
    filename = function(){
      paste("climateChangeData", "zip", sep=".")
    },
    
    content = function(file) {
      
      # Create Temporary Folder for Storing files
      time = Sys.time()
      folder_name = format(time, format = "%Y_%b_%d_%H_%M_%S")
      if (file.exists(folder_name)){
        i = 0
        folder_name_new = paste(folder_name, "_", i)
        while (file.exists(folder_name_new)){
          folder_name_new = paste(folder_name, "_", i+1)
          i = i+1
        }
        folder_name = folder_name_new
      }
      dir.create(folder_name)
      out_dirs <<- c(out_dirs, folder_name)
      files = c()
      
      fnames = c()
      
      #parent_folder = "/mnt/pd01/Research_data/Personel/Narayan/TmaxTminPrecDataObserved/"
      parent_folder = "D:/Paper_Climate/Data/siguiente paper/App/app2/pcp_tmp/"
      
      withProgress(message = "Preparing data for download - Please Wait", value=0, {
        for (param in input$dataGroup){
          if (param == 1){
            fname = "pcp_Data_1981_to_2016/"
            master_fname = paste(parent_folder, fname, "pcp.txt", sep = "")
            locFname = paste("./", folder_name, "/pcp.txt", sep = "")
            # Write metainformation to readme file
            metaFname = paste("./", folder_name, "/pcp_README.txt", sep = "")
            meta_data = c()
            meta_data = append(meta_data, "PARAMETER: PRECIPITATION")
            meta_data = append(meta_data, "UNIT: MM/DAY")
            meta_data = append(meta_data, "PERIOD: 1981-2016")
            meta_data = append(meta_data, "SPATIAL RESOLUTION: 300 ARC SECONDS (~10 KM)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "ORIGINAL DATA CONTRIBUTOR: LANDSCAPE ANALYSIS AND APPLICATION SECTION (LAAS), GREAT LAKES FORESTRY CENTRE (GLFC), CANADIAN FOREST SERVICE (CFS),
                                                                      NATURAL RESOURCES CANADA (NRCAN)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "DATA CITATIONS:")
            meta_data = append(meta_data, "1) FOR ORIGINAL DATA: a) Hopkinson, R. F., D. W. McKenney, E. J. Milewska, M. F. Hutchinson, P. Papadopol, and L. A. Vincent, 2011: Impact of Aligning Climatological Day on Gridding Daily Maximum-Minimum Temperature and Precipitation over Canada. J. Appl. Meteor. Climatol., 50, 1654-1665, https://doi.org/10.1175/2011JAMC2684.1.
                                                                 b) Hutchinson, M. F., D. W. McKenney, K. Lawrence, J. H. Pedlar, R. F. Hopkinson, E. Milewska, and P. Papadopol, 2009: Development and Testing of Canada-Wide Interpolated Spatial Models of Daily Minimum-Maximum Temperature and Precipitation for 1961-2003. J. Appl. Meteor. Climatol., 48, 725-741, https://doi.org/10.1175/2008JAMC1979.1.
                                                                 c) McKenney DW, Hutchinson MF, Papadopol P, Lawrence K, Pedlar J, Campbell K, et al. Customized Spatial Climate Models for North America. Bulletin of the American Meteorological Society 2011; 92: 1611-1622.")
            meta_data = append(meta_data, "2) FOR CanGLWS DATA SERVICE: TO-DO")
            write.table(meta_data, metaFname, row.names = FALSE, sep = "\n", quote = FALSE, col.names=FALSE)
            files = c(files, metaFname)
          }
          
          else if (param == 2){
            fname = "tmp_Data_1950_to_2015/"
            master_fname = paste(parent_folder, fname, "tmp.txt", sep = "")
            locFname = paste("./", folder_name, "/tmp.txt", sep = "")
            # Write metainformation to readme file
            metaFname = paste("./", folder_name, "/tmp_README.txt", sep = "")
            meta_data = c()
            meta_data = append(meta_data, "PARAMETER: MAXIMUM AND MINIMUM AIR TEMPERATURE")
            meta_data = append(meta_data, "UNIT: DEGREE CELCIUS")
            meta_data = append(meta_data, "PERIOD: 1950-2015")
            meta_data = append(meta_data, "SPATIAL RESOLUTION: 300 ARC SECONDS (~10 KM)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "ORIGINAL DATA CONTRIBUTOR: LANDSCAPE ANALYSIS AND APPLICATION SECTION (LAAS), GREAT LAKES FORESTRY CENTRE (GLFC), CANADIAN FOREST SERVICE (CFS),
                                                                      NATURAL RESOURCES CANADA (NRCAN)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "DATA CITATIONS:")
            meta_data = append(meta_data, "1) FOR RAW WEATHER DATA: a) Hopkinson, R. F., D. W. McKenney, E. J. Milewska, M. F. Hutchinson, P. Papadopol, and L. A. Vincent, 2011: Impact of Aligning Climatological Day on Gridding Daily Maximum-Minimum Temperature and Precipitation over Canada. J. Appl. Meteor. Climatol., 50, 1654-1665, https://doi.org/10.1175/2011JAMC2684.1.
                                                                    b) Hutchinson, M. F., D. W. McKenney, K. Lawrence, J. H. Pedlar, R. F. Hopkinson, E. Milewska, and P. Papadopol, 2009: Development and Testing of Canada-Wide Interpolated Spatial Models of Daily Minimum-Maximum Temperature and Precipitation for 1961-2003. J. Appl. Meteor. Climatol., 48, 725-741, https://doi.org/10.1175/2008JAMC1979.1.
                                                                    c) McKenney DW, Hutchinson MF, Papadopol P, Lawrence K, Pedlar J, Campbell K, et al. Customized Spatial Climate Models for North America. Bulletin of the American Meteorological Society 2011; 92: 1611-1622.")
            meta_data = append(meta_data, "2) FOR WEATHER STATISTICS DATA: TO-DO")
            meta_data = append(meta_data, "3) FOR CanGLWS DATA SERVICE: TO-DO")
            write.table(meta_data, metaFname, row.names = FALSE, sep = "\n", quote = FALSE, col.names=FALSE)
            files = c(files, metaFname)
          }
          
          else if (param == 3){        ######### empezamos de aqui para lel viento ,rh y solar
            fname = "wind_Data_1950_to_2015/"
            master_fname = paste(parent_folder, fname, "wind.txt", sep = "")
            locFname = paste("./", folder_name, "/wind.txt", sep = "")
            # Write metainformation to readme file
            metaFname = paste("./", folder_name, "/wind_README.txt", sep = "")
            meta_data = c()
            meta_data = append(meta_data, "PARAMETER: WIND SPEED")
            meta_data = append(meta_data, "UNIT: M/S")
            meta_data = append(meta_data, "PERIOD: 1950-2015")
            meta_data = append(meta_data, "SPATIAL RESOLUTION: 300 ARC SECONDS (~10 KM)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "ORIGINAL DATA CONTRIBUTOR: LANDSCAPE ANALYSIS AND APPLICATION SECTION (LAAS), GREAT LAKES FORESTRY CENTRE (GLFC), CANADIAN FOREST SERVICE (CFS), NATURAL RESOURCES CANADA (NRCAN)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "DATA CITATIONS:")
            meta_data = append(meta_data, "1) FOR ORIGINAL DATA: a) Hopkinson, R. F., D. W. McKenney, E. J. Milewska, M. F. Hutchinson, P. Papadopol, and L. A. Vincent, 2011: Impact of Aligning Climatological Day on Gridding Daily Maximum-Minimum Temperature and Precipitation over Canada. J. Appl. Meteor. Climatol., 50, 1654-1665, https://doi.org/10.1175/2011JAMC2684.1. b) Hutchinson, M. F., D. W. McKenney, K. Lawrence, J. H. Pedlar, R. F. Hopkinson, E. Milewska, and P. Papadopol, 2009: Development and Testing of Canada-Wide Interpolated Spatial Models of Daily Minimum-Maximum Temperature and Precipitation for 1961-2003. J. Appl. Meteor. Climatol., 48, 725-741, https://doi.org/10.1175/2008JAMC1979.1. c) McKenney DW, Hutchinson MF, Papadopol P, Lawrence K, Pedlar J, Campbell K, et al. Customized Spatial Climate Models for North America. Bulletin of the American Meteorological Society 2011; 92: 1611-1622.")
            meta_data = append(meta_data, "2) FOR CanGLWS DATA SERVICE: TO-DO")
            write.table(meta_data, metaFname, row.names = FALSE, sep = "\n", quote = FALSE, col.names=FALSE)
            files = c(files, metaFname)
          }
          
          else if (param == 4){
            fname = "rh_Data_1950_to_2015/"
            master_fname = paste(parent_folder, fname, "rh.txt", sep = "")
            locFname = paste("./", folder_name, "/rh.txt", sep = "")
            # Write metainformation to readme file
            metaFname = paste("./", folder_name, "/rh_README.txt", sep = "")
            meta_data = c()
            meta_data = append(meta_data, "PARAMETER: RELATIVE HUMIDITY")
            meta_data = append(meta_data, "UNIT: %")
            meta_data = append(meta_data, "PERIOD: 1950-2015")
            meta_data = append(meta_data, "SPATIAL RESOLUTION: 300 ARC SECONDS (~10 KM)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "ORIGINAL DATA CONTRIBUTOR: LANDSCAPE ANALYSIS AND APPLICATION SECTION (LAAS), GREAT LAKES FORESTRY CENTRE (GLFC), CANADIAN FOREST SERVICE (CFS), NATURAL RESOURCES CANADA (NRCAN)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "DATA CITATIONS:")
            meta_data = append(meta_data, "1) FOR ORIGINAL DATA: a) Hopkinson, R. F., D. W. McKenney, E. J. Milewska, M. F. Hutchinson, P. Papadopol, and L. A. Vincent, 2011: Impact of Aligning Climatological Day on Gridding Daily Maximum-Minimum Temperature and Precipitation over Canada. J. Appl. Meteor. Climatol., 50, 1654-1665, https://doi.org/10.1175/2011JAMC2684.1. b) Hutchinson, M. F., D. W. McKenney, K. Lawrence, J. H. Pedlar, R. F. Hopkinson, E. Milewska, and P. Papadopol, 2009: Development and Testing of Canada-Wide Interpolated Spatial Models of Daily Minimum-Maximum Temperature and Precipitation for 1961-2003. J. Appl. Meteor. Climatol., 48, 725-741, https://doi.org/10.1175/2008JAMC1979.1. c) McKenney DW, Hutchinson MF, Papadopol P, Lawrence K, Pedlar J, Campbell K, et al. Customized Spatial Climate Models for North America. Bulletin of the American Meteorological Society 2011; 92: 1611-1622.")
            meta_data = append(meta_data, "2) FOR CanGLWS DATA SERVICE: TO-DO")
            write.table(meta_data, metaFname, row.names = FALSE, sep = "\n", quote = FALSE, col.names=FALSE)
            files = c(files, metaFname)
          }
          
          else if (param == 5){
            fname = "solar_Data_1950_to_2015/"
            master_fname = paste(parent_folder, fname, "solar.txt", sep = "")
            locFname = paste("./", folder_name, "/solar.txt", sep = "")
            # Write metainformation to readme file
            metaFname = paste("./", folder_name, "/solar_README.txt", sep = "")
            meta_data = c()
            meta_data = append(meta_data, "PARAMETER: SOLAR RADIATION")
            meta_data = append(meta_data, "UNIT: MJ/M2")
            meta_data = append(meta_data, "PERIOD: 1950-2015")
            meta_data = append(meta_data, "SPATIAL RESOLUTION: 300 ARC SECONDS (~10 KM)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "ORIGINAL DATA CONTRIBUTOR: LANDSCAPE ANALYSIS AND APPLICATION SECTION (LAAS), GREAT LAKES FORESTRY CENTRE (GLFC), CANADIAN FOREST SERVICE (CFS), NATURAL RESOURCES CANADA (NRCAN)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "DATA CITATIONS:")
            meta_data = append(meta_data, "1) FOR ORIGINAL DATA: a) Hopkinson, R. F., D. W. McKenney, E. J. Milewska, M. F. Hutchinson, P. Papadopol, and L. A. Vincent, 2011: Impact of Aligning Climatological Day on Gridding Daily Maximum-Minimum Temperature and Precipitation over Canada. J. Appl. Meteor. Climatol., 50, 1654-1665, https://doi.org/10.1175/2011JAMC2684.1. b) Hutchinson, M. F., D. W. McKenney, K. Lawrence, J. H. Pedlar, R. F. Hopkinson, E. Milewska, and P. Papadopol, 2009: Development and Testing of Canada-Wide Interpolated Spatial Models of Daily Minimum-Maximum Temperature and Precipitation for 1961-2003. J. Appl. Meteor. Climatol., 48, 725-741, https://doi.org/10.1175/2008JAMC1979.1. c) McKenney DW, Hutchinson MF, Papadopol P, Lawrence K, Pedlar J, Campbell K, et al. Customized Spatial Climate Models for North America. Bulletin of the American Meteorological Society 2011; 92: 1611-1622.")
            meta_data = append(meta_data, "2) FOR CanGLWS DATA SERVICE: TO-DO")
            write.table(meta_data, metaFname, row.names = FALSE, sep = "\n", quote = FALSE, col.names=FALSE)
            files = c(files, metaFname)
          } 
          
          ######### empezamos de aqui para lel viento ,rh y solar

          fnames = append(fnames, fname)
          my_files = read.table(master_fname, header = TRUE, sep = ",")
          sub = paste(my_files$LAT, my_files$LONG, sep=":") %in% paste(selected$Y_coord, selected$X_coord, sep=":")
          selected_files = my_files[sub,]
          
          write.table(selected_files, locFname, row.names = FALSE, sep = ",", quote = FALSE)
          files = c(files, locFname)
          
          start_index = as.numeric(input$dateRange[1] - as.Date("1981-01-01")) + 1
          end_index = as.numeric(input$dateRange[2] - as.Date("1981-01-01")) + 1
          
          for (name in selected_files$NAME){
            source_file = paste(parent_folder, fname, name, ".txt", sep = "")
            dest_file = paste("./", folder_name, "/", name, ".txt", sep = "")
            my_data = read.table(source_file, header = TRUE, sep = "", stringsAsFactors = F)
            start_date = format(input$dateRange[1], format = "%Y%m%d")
            my_filtered_data = c(start_date, my_data[start_index:end_index,])
            write.table(my_filtered_data, dest_file, row.names = FALSE, quote = FALSE, col.names = FALSE)
            files <- c(files, dest_file)
          }
        }
      })
      
      zip::zipr(zipfile=file, files=files)
    }
    
  )
  
  ########################################################################
  ## UI response start for for historical Data Selection --------------
  ########################################################################
  
  output$hist_map <- renderLeaflet({
    basemap
  })
  
  observeEvent(input$hist_radio,{
    choice <- input$hist_radio
    if (choice == 1){
      proxy <- leafletProxy("map")
      proxy %>% clearMarkers()
    }
    else{
      proxy <- leafletProxy("map")
      proxy %>% clearMarkers()
    }
  })
  
  # Selection of custom drawn map 
  observeEvent(input$hist_map_draw_new_feature,{
    
    # Step 1 - Filter data points within region
    
    found_in_bounds <- findLocations(shape = input$hist_map_draw_new_feature
                                     , location_coordinates = hist_points
                                     , location_id_colname = "SN")
    hist_selected <<- subset(hist_points, SN %in% found_in_bounds)
    
    
    # Step 3 - Print Data Summary of Selected Data and Paste their Locations    
    # output$selected_var <- renderText({ 
    #   paste("Number of Observations Found: ",length(selected))
    # })
    proxy <- leafletProxy("hist_map")
    proxy %>% clearMarkers()
    #proxy %>% setView(lng=viewCenter[1], lat = viewCenter[2], input$Map_zoom)
    proxy %>% addCircleMarkers(data = hist_selected,
                               radius = 3,
                               lat = hist_selected$Y_coord,
                               lng = hist_selected$X_coord,
                               fillOpacity = 0.5,
                               color = "blue",
                               stroke = FALSE,
                               layerId = ~SN
    )
    
    if (length(hist_selected) > 0){
      if (is.null(input$hist_dataGroup)){
        shinyjs::disable("hist_downloadClimate")
      }
      else {shinyjs::enable("hist_downloadClimate")}
    }
    
  })
  
  # The following code handles the case where shape file is provided by user
  observeEvent(input$hist_shpFile, {
    
    if (!is.null(error_id))
      removeNotification(error_id)
    error_id <<- NULL
    
    inFile <- input$hist_shpFile
    if (is.null(inFile))
      return(NULL)
    
    # Once the filepath is known, copy file to server folder
    withProgress(message = "Uploading and unzipping shape file - Please Wait", value=0,{
      #file.copy(inFile$datapath, file.path(getwd(), inFile$name))
      unlink("./Scratch", recursive = TRUE)
      dir.create("Scratch")
      unzip(inFile$datapath, exdir = "./Scratch")
    })
    
    # Check shape file format and load shape file
    dbfFiles <- list.files(path="./Scratch", pattern = "\\.dbf$")
    if (length(dbfFiles) == 0){
      error_id <<- showNotification("No Shape File Found !!!!", type = "error")
    }
    else if (length(dbfFiles) > 1){
      error_id <<- showNotification("Multiple Shape Files found. Choosing first file.", type = "warning")
      withProgress(message = 'Loading Shapes - Please Wait', value=0,{
        shpName <- strsplit(dbfFiles[1], '.')
        customShape <- readOGR(dsn="./Scratch", layer=tools::file_path_sans_ext(dbfFiles[1]))
        customShape <- spTransform(customShape,CRS("+proj=longlat +datum=WGS84"))
        
      })
    }
    else {
      withProgress(message = 'Loading Shapes - Please Wait', value=0,{
        customShape <- readOGR(dsn="./Scratch", layer=tools::file_path_sans_ext(dbfFiles[1]))
        customShape <- spTransform(customShape,CRS("+proj=longlat +datum=WGS84"))
      })
    }
    
    # Display shapefile on map
    if (length(customShape) > 1){ customShape <- customShape[1]}
    
    newP = customShape@polygons
    selected_locs <- sp::over(hist_points, sp::SpatialPolygons(newP))
    x = (hist_points[which(!is.na(selected_locs)), "SN"])
    selected_loc_id = as.character(x[["SN"]])
    hist_selected <<- subset(hist_points, SN %in% selected_loc_id)
    
    
    bounds <- sp::bbox(customShape)
    proxy <- leafletProxy("hist_map")
    proxy %>% clearMarkers()
    proxy %>% addPolygons(data=customShape, color = "red", fillColor = "transparent", weight = 4.0)
    proxy %>% addCircleMarkers(data = hist_selected,
                               radius = 3,
                               lat = hist_selected$Y_coord,
                               lng = hist_selected$X_coord,
                               fillOpacity = 0.5,
                               color = "blue",
                               stroke = FALSE,
                               layerId = ~SN
    )
    proxy %>% fitBounds(lng1 = bounds[1,1], lat1 = bounds[2,1], lng2 = bounds[1,2], lat2 = bounds[2,2])
    
    if (length(hist_selected) > 0){
      if (is.null(input$hist_dataGroup)){
        shinyjs::disable("hist_downloadClimate")
      }
      else {shinyjs::enable("hist_downloadClimate")}
    }
    
  })
  
  # Downloadable selected climate change data ----
  output$hist_downloadClimate <- downloadHandler(
    filename = function(){
      paste("historicalData", "zip", sep=".")
    },
    
    content = function(file) {
      
      # Create Temporary Folder for Storing files
      time = Sys.time()
      folder_name = format(time, format = "%Y_%b_%d_%H_%M_%S")
      if (file.exists(folder_name)){
        i = 0
        folder_name_new = paste(folder_name, "_", i)
        while (file.exists(folder_name_new)){
          folder_name_new = paste(folder_name, "_", i+1)
          i = i+1
        }
        folder_name = folder_name_new
      }
      dir.create(folder_name)
      out_dirs <<- c(out_dirs, folder_name)
      files = c()
      
      #parent_folder = "/mnt/pd01/Research_data/Personel/Narayan/TmaxTminPrecDataObserved/"
      parent_folder = "D:/Paper_Climate/Data/siguiente paper/App/app2/pcp_tmp/"
      
      withProgress(message = "Preparing data for download - Please Wait", value=0, {
        for (param in input$hist_dataGroup){
          if (param == 1){
            fname = "pcp_Data_1981_to_2016/"
            master_fname = paste(parent_folder, fname, "pcp.txt", sep = "")
            locFname = paste("./", folder_name, "/pcp.txt", sep = "")
            # Write metainformation to readme file
            metaFname = paste("./", folder_name, "/pcp_README.txt", sep = "")
            meta_data = c()
            meta_data = append(meta_data, "PARAMETER: PRECIPITATION")
            meta_data = append(meta_data, "UNIT: MM/DAY")
            meta_data = append(meta_data, "PERIOD: 1950-2015")
            meta_data = append(meta_data, "SPATIAL RESOLUTION: 300 ARC SECONDS (~10 KM)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "ORIGINAL DATA CONTRIBUTOR: LANDSCAPE ANALYSIS AND APPLICATION SECTION (LAAS), GREAT LAKES FORESTRY CENTRE (GLFC), CANADIAN FOREST SERVICE (CFS),
                                                                      NATURAL RESOURCES CANADA (NRCAN)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "DATA CITATIONS:")
            meta_data = append(meta_data, "1) FOR ORIGINAL DATA: a) Hopkinson, R. F., D. W. McKenney, E. J. Milewska, M. F. Hutchinson, P. Papadopol, and L. A. Vincent, 2011: Impact of Aligning Climatological Day on Gridding Daily Maximum-Minimum Temperature and Precipitation over Canada. J. Appl. Meteor. Climatol., 50, 1654-1665, https://doi.org/10.1175/2011JAMC2684.1.
                                                                 b) Hutchinson, M. F., D. W. McKenney, K. Lawrence, J. H. Pedlar, R. F. Hopkinson, E. Milewska, and P. Papadopol, 2009: Development and Testing of Canada-Wide Interpolated Spatial Models of Daily Minimum-Maximum Temperature and Precipitation for 1961-2003. J. Appl. Meteor. Climatol., 48, 725-741, https://doi.org/10.1175/2008JAMC1979.1.
                                                                 c) McKenney DW, Hutchinson MF, Papadopol P, Lawrence K, Pedlar J, Campbell K, et al. Customized Spatial Climate Models for North America. Bulletin of the American Meteorological Society 2011; 92: 1611-1622.")
            meta_data = append(meta_data, "2) FOR CanGLWS DATA SERVICE: TO-DO")
            write.table(meta_data, metaFname, row.names = FALSE, sep = "\n", quote = FALSE, col.names=FALSE)
            files = c(files, metaFname)
          }
          
          else if (param == 2){
            fname = "tmp_Data_1950_to_2015/"
            master_fname = paste(parent_folder, fname, "tmp.txt", sep = "")
            locFname = paste("./", folder_name, "/tmp.txt", sep = "")
            # Write metainformation to readme file
            metaFname = paste("./", folder_name, "/tmp_README.txt", sep = "")
            meta_data = c()
            meta_data = append(meta_data, "PARAMETER: MAXIMUM AND MINIMUM AIR TEMPERATURE")
            meta_data = append(meta_data, "UNIT: DEGREE CELCIUS")
            meta_data = append(meta_data, "PERIOD: 1950-2015")
            meta_data = append(meta_data, "SPATIAL RESOLUTION: 300 ARC SECONDS (~10 KM)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "ORIGINAL DATA CONTRIBUTOR: LANDSCAPE ANALYSIS AND APPLICATION SECTION (LAAS), GREAT LAKES FORESTRY CENTRE (GLFC), CANADIAN FOREST SERVICE (CFS),
                                                                      NATURAL RESOURCES CANADA (NRCAN)")
            meta_data = append(meta_data, "")
            meta_data = append(meta_data, "DATA CITATIONS:")
            meta_data = append(meta_data, "1) FOR RAW WEATHER DATA: a) Hopkinson, R. F., D. W. McKenney, E. J. Milewska, M. F. Hutchinson, P. Papadopol, and L. A. Vincent, 2011: Impact of Aligning Climatological Day on Gridding Daily Maximum-Minimum Temperature and Precipitation over Canada. J. Appl. Meteor. Climatol., 50, 1654-1665, https://doi.org/10.1175/2011JAMC2684.1.
                                                                    b) Hutchinson, M. F., D. W. McKenney, K. Lawrence, J. H. Pedlar, R. F. Hopkinson, E. Milewska, and P. Papadopol, 2009: Development and Testing of Canada-Wide Interpolated Spatial Models of Daily Minimum-Maximum Temperature and Precipitation for 1961-2003. J. Appl. Meteor. Climatol., 48, 725-741, https://doi.org/10.1175/2008JAMC1979.1.
                                                                    c) McKenney DW, Hutchinson MF, Papadopol P, Lawrence K, Pedlar J, Campbell K, et al. Customized Spatial Climate Models for North America. Bulletin of the American Meteorological Society 2011; 92: 1611-1622.")
            meta_data = append(meta_data, "2) FOR WEATHER STATISTICS DATA: TO-DO")
            meta_data = append(meta_data, "3) FOR CanGLWS DATA SERVICE: TO-DO")
            write.table(meta_data, metaFname, row.names = FALSE, sep = "\n", quote = FALSE, col.names=FALSE)
            files = c(files, metaFname)
          }
          
          my_files = read.table(master_fname, header = TRUE, sep = ",")
          sub = paste(my_files$LAT, my_files$LONG, sep=":") %in% paste(hist_selected$Y_coord, hist_selected$X_coord, sep=":")
          selected_files = my_files[sub,]
          
          write.table(selected_files, locFname, row.names = FALSE, sep = ",", quote = FALSE)
          files = c(files, locFname)
          
          start_index = as.numeric(input$hist_dateRange[1] - as.Date("1981-01-01")) + 1
          end_index = as.numeric(input$hist_dateRange[2] - as.Date("1981-01-01")) + 1
          
          for (name in selected_files$NAME){
            source_file = paste(parent_folder, fname, name, ".txt", sep = "")
            dest_file = paste("./", folder_name, "/", name, ".txt", sep = "")
            my_data = read.table(source_file, header = TRUE, sep = "", stringsAsFactors = F)
            start_date = format(input$hist_dateRange[1], format = "%Y%m%d")
            my_filtered_data = c(start_date, my_data[start_index:end_index,])
            write.table(my_filtered_data, dest_file, row.names = FALSE, quote = FALSE, col.names = FALSE)
            files <- c(files, dest_file)
          }
        }
      })
      
      zip::zipr(zipfile=file, files=files)
    }
    
  )
  ## UI response END for for historical Data Selection --------------
  
  ##########################################################################
  ## UI response START for Statistical Data Selection --------------
  ##########################################################################
  
  output$stats_map <- renderLeaflet({
    basemap
  })
  
  # Selection of custom drawn map 
  observeEvent(input$stats_map_draw_new_feature,{
    
    # Step 0 - Load statistics file
    if (is.null(stats_all)){
      stats_all <<- get_climate_statistics()
    }
    
    # Step 1 - Filter data points within region
    
    found_in_bounds <- findLocations(shape = input$stats_map_draw_new_feature
                                     , location_coordinates = stats_all
                                     , location_id_colname = "OBJECTID")
    stats_selected <- subset(stats_all, OBJECTID %in% found_in_bounds)
    
    
    # Step 3 - Print Data Summary of Selected Data and Paste their Locations    
    proxy <- leafletProxy("stats_map")
    proxy %>% clearMarkers()
    proxy %>% addCircleMarkers(data = stats_selected,
                               radius = 3,
                               lat = stats_selected$WLATITUDE,
                               lng = stats_selected$WLONGITUDE,
                               fillOpacity = 0.5,
                               color = "blue",
                               stroke = FALSE,
                               layerId = ~OBJECTID
    )
    
    if (length(stats_selected) > 0){
      shinyjs::enable("stats_download")
    }
    
    sdata <<-as.data.frame(stats_selected)
    sdata <<- sdata[,1:(ncol(sdata)-3)]
    
  })
  
  # The following code handles the case where shape file is provided by user
  observeEvent(input$stats_shpFile, {
    
    if (!is.null(error_id))
      removeNotification(error_id)
    error_id <<- NULL
    
    inFile <- input$stats_shpFile
    if (is.null(inFile))
      return(NULL)
    
    # Once the filepath is known, copy file to server folder
    withProgress(message = "Uploading and unzipping shape file - Please Wait", value=0,{
      #file.copy(inFile$datapath, file.path(getwd(), inFile$name))
      unlink("./Scratch", recursive = TRUE)
      dir.create("Scratch")
      unzip(inFile$datapath, exdir = "./Scratch")
    })
    
    # Check shape file format and load shape file
    dbfFiles <- list.files(path="./Scratch", pattern = "\\.dbf$")
    if (length(dbfFiles) == 0){
      error_id <<- showNotification("No Shape File Found !!!!", type = "error")
    }
    else if (length(dbfFiles) > 1){
      error_id <<- showNotification("Multiple Shape Files found. Choosing first file.", type = "warning")
      withProgress(message = 'Loading Shapes - Please Wait', value=0,{
        shpName <- strsplit(dbfFiles[1], '.')
        customShape <- readOGR(dsn="./Scratch", layer=tools::file_path_sans_ext(dbfFiles[1]))
        customShape <- spTransform(customShape,CRS("+proj=longlat +datum=WGS84"))
        
      })
    }
    else {
      withProgress(message = 'Loading Shapes - Please Wait', value=0,{
        customShape <- readOGR(dsn="./Scratch", layer=tools::file_path_sans_ext(dbfFiles[1]))
        customShape <- spTransform(customShape,CRS("+proj=longlat +datum=WGS84"))
      })
    }
    
    # Display shapefile on map
    if (length(customShape) > 1){ customShape <- customShape[1]}
    
    # Step 0 - Load statistics file
    if (is.null(stats_all)){
      stats_all <<- get_climate_statistics()
    }
    
    newP = customShape@polygons
    selected_locs <- sp::over(stats_all, sp::SpatialPolygons(newP))
    x = (stats_all[which(!is.na(selected_locs)), "OBJECTID"])
    selected_loc_id = as.character(x[["OBJECTID"]])
    stats_selected <- subset(stats_all, OBJECTID %in% selected_loc_id)
    
    
    bounds <- sp::bbox(customShape)
    proxy <- leafletProxy("stats_map")
    proxy %>% clearMarkers()
    proxy %>% addPolygons(data=customShape, color = "red", fillColor = "transparent", weight = 4.0)
    proxy %>% addCircleMarkers(data = stats_selected,
                               radius = 3,
                               lat = stats_selected$WLATITUDE,
                               lng = stats_selected$WLONGITUDE,
                               fillOpacity = 0.5,
                               color = "blue",
                               stroke = FALSE,
                               layerId = ~OBJECTID
    )
    proxy %>% fitBounds(lng1 = bounds[1,1], lat1 = bounds[2,1], lng2 = bounds[1,2], lat2 = bounds[2,2])
    
    if (length(stats_selected) > 0){
      shinyjs::enable("stats_download")
    }
    
    sdata <<-as.data.frame(stats_selected)
    sdata <<- sdata[,1:(ncol(sdata)-3)]
    
  })
  
  # Downloadable selected climate change data ----
  output$stats_download <- downloadHandler(
    filename = "WGN_Data.csv",
    content = function(file) {
      write.csv(sdata, file, row.names = FALSE)
    }
    
  )
  
  ## UI response END for for Statistical Data Selection --------------
  
  session$onSessionEnded(
    function(){
      i = 1
      while (i <= length(out_dirs)){
        unlink(paste("./", out_dirs[i], sep=""), recursive = TRUE)
        i = i + 1
      }
    }
  )
  
}

shinyApp(ui = ui, server = server)



