#' weather_data UI Function
#'
#' @description A shiny Module implementing a request-based
#' weather data acquisition service
#'
#' @param id,input,output,session Internal parameters for {shiny}.
#'
#' @noRd 
#'
#' @importFrom shiny NS tagList 
mod_request_weather_ui <- function(id){
  ns <- NS(id)
  tagList(
    div(
      class = "outer",
      shinyjs::useShinyjs(),
      editModUI(ns("weather_data_editor"), width="100%", height="100%"),
      
      absolutePanel(
        id = "controls", class = "panel panel-default",
        fixed = TRUE, draggable = TRUE, top = 90,
        left = "auto", right = 200, bottom = "auto",
        width = 350, height = "auto",
        h2("Data Selector"),
        
        checkboxGroupInput(ns("wd_dataGroup"), label = h4("Parameters:"), 
                           choices = list("Precipitation (mm)" = 1,
                                          "Temperature (C)" = 2),
                           selected = 1, inline = TRUE),
        
        dateRangeInput(ns('wd_dateRange'),
                       label = h4('Date Range:'),
                       start = as.Date("2001-01-01"),
                       end = as.Date("2019-12-31"),
                       min = as.Date("2001-01-01"),
                       max = as.Date("2020-03-26")),
        
        radioButtons(ns("wd_radio"), label = "Upload Shape File?",
                     choices = c("Yes" = 1, "No" = 2), 
                     inline=TRUE, selected = 2),
        
        conditionalPanel(condition = "input.wd_radio == 1", ns = ns,
                         mod_upload_shape_ui(ns("upload_shape_ui_1"))),
        
        radioButtons(ns("wd_format"), label = "Format of downloaded data?",
                     choices = c("SWAT format" = 1, "HEC/CSV format" = 2), 
                     inline=TRUE, selected = 1),
        
        disabled(actionButton(ns("wd_reqData"), "Request Data", icon("cloud"))),
        
        HTML("<br><br>"),
        tags$div(id = 'placeholder'),
        tags$div(id = 'placeholder_1')
        
      )
    )
  )
}

#' weather_data Server Function
#'
#' @noRd 
mod_request_weather_server <- function(input, output, session){
  ns <- session$ns
  
  ## declare variables
  jobParams <- list(
    email = NULL,
    start_date = NULL,
    end_date = NULL,
    data_type = "Historical",
    data_format = "SWAT",
    var_flag = NULL
  )
  selArea <- NULL
  selectionStage <- 0
  
  basemap <- leaflet() %>%
    addProviderTiles(providers$CartoDB.Positron) %>%
    setView(0, 0, 2)
  
  # addTiles(
  #   urlTemplate = "https://tiles.stadiamaps.com/tiles/osm_bright/{z}/{x}/{y}{r}.png",
  #   attribution = '&copy; <a href="https://stadiamaps.com/">Stadia Maps</a>, &copy; <a href="https://openmaptiles.org/">OpenMapTiles</a> &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors'
  # ) %>%
  #addProviderTiles(providers$Esri.WorldStreetMap) %>%
  
  edit_opts <- list(circleOptions = FALSE,
                    markerOptions = FALSE,
                    circleMarkerOptions = FALSE,
                    polylineOptions=FALSE,
                    singleFeature = TRUE,
                    targetGroup='Selected')
  
  
  edits <- callModule(editMod, "weather_data_editor",
                      basemap, editorOptions = edit_opts)
  
  customShp <- callModule(mod_upload_shape_server, "upload_shape_ui_1")
  
  observe({
    # If shape file is to be drawn drawn  
    if(input$wd_radio == 2){
      if(!(is.null(edits()$finished))){
        if(selectionStage == 0){
          selArea <<- edits()$finished
          if (as.numeric(st_area(selArea)) < 10^12){
            shinyjs::enable("wd_reqData")
          }
          else {
            shinyalert(
              title = "Invalid Area",
              text = "The selected area is too big. Kindly choose an area smaller than 10M Hectares.",
              type = "error"
            )
          }
        }
      }
      else{
        shinyjs::disable("wd_reqData")
      }
    }
    # If shape file is to be uploaded  
    else{
      if(!is.null(customShp())){
        if(selectionStage == 0){
          selArea <<- customShp()
          bounds <- st_bbox(selArea)
          proxy <- leafletProxy("weather_data_editor-map")
          proxy %>% clearMarkers()
          proxy %>% addPolygons(data=selArea, color = "red",
                                fillColor = "red",fillOpacity = 0.2,
                                weight = 2.0)
          proxy %>% fitBounds(bounds$xmin[[1]], bounds$ymin[[1]],
                              bounds$xmax[[1]], bounds$ymax[[1]])
          if (as.numeric(st_area(selArea)) < 10^12){
            shinyjs::enable("wd_reqData")
          }
          else {
            shinyalert(
              title = "Invalid Area",
              text = "The selected area is too big. Kindly choose an area smaller than 10M Hectares.",
              type = "error"
            )
          }
        }
      }
      else{
        shinyjs::disable("wd_reqData")
      }
    }
  })
  
  observeEvent(input$wd_reqData,{
    
    selectionStage <<- 1
    shinyjs::disable("wd_reqData")
    
    sendSweetAlert(
      session = session,
      title = "Email required",
      text =  paste("It may take a few hours or days to process your data.",
                    "We will email download instructions once the data is processed.",
                    "If you agree to share your email please enter it below.", 
                    "Your email will only be used for data sharing.")
    )
    
    insertUI(
      selector = "#placeholder",
      ui = textInput(ns("email_1"),
                     "Provide Email")
    )
    insertUI(
      selector = "#placeholder",
      ui = textInput(ns("email_2"),
                     "Confirm Email")
    )
    insertUI(
      selector = "#placeholder",
      ui = actionButton(ns("wd_submit"),
                        "Submit Request", icon("cloud"))
    )
    
  })
  
  observeEvent(input$wd_submit,{
    if(!(input$email_1 == "") & (input$email_1 == input$email_2)){
      
      # Remove new UI elements
      button_selector = paste0('div:has(> #', ns('wd_submit'), ')')
      email_1_selector = paste0('div:has(> #', ns('email_1'), ')')
      email_2_selector = paste0('div:has(> #', ns('email_2'), ')')
      removeUI(
        selector = button_selector
      )
      removeUI(
        selector = email_1_selector
      )
      removeUI(
        selector = email_2_selector
      )
      
      # Submit job for processing
      jobParams$email <<- input$email_1
      jobParams$start_date <<- format(input$wd_dateRange[1], format = "%Y-%m-%d")
      jobParams$end_date <<- format(input$wd_dateRange[2], format = "%Y-%m-%d")
      if(1 %in% input$wd_dataGroup){
        if(2 %in% input$wd_dataGroup){
          jobParams$var_flag <<- 2
        }
        else{
          jobParams$var_flag <<- 1    
        }
      }
      else{
        if(2 %in% input$wd_dataGroup){
          jobParams$var_flag <<- 3
        }
      }
      if(!length(input$wd_dataGroup)){jobParams$var_flag <<- 1}
      if(input$wd_format == 2){jobParams$data_format <<- "CSV"}
      
      jobId <- create_job_id()
      status <- send_submission_email(jobId, jobParams$email)
      print(class(status))
      if(class(status) == 'try-error'){
        sendSweetAlert(
          session = session,
          title = "Failed",
          text = "Email was not sent successfully. Please provide a valid email.",
          type = "error"
        )
      }
      else{
        submit_job(jobId, jobParams, selArea)
        sendSweetAlert(
          session = session,
          title = "Submitted",
          text = "Your data request has been submitted. You will shortly
        receive an email confirmation.",
          type = "success"
        )
      }
      
      insertUI(
        selector = "#placeholder_1",
        ui = actionButton(ns("wd_reset"),
                          "Refresh App", icon("redo"))
      )
      
    }
    else{
      sendSweetAlert(
        session = session,
        title = "Invalid email",
        text = "The email address entered is not valid.",
        type = "error"
      )
    }
  })
  
  observeEvent(input$wd_reset, {
    session$reload()
  })
  
  
}

## To be copied in the UI
# mod_reqeust_weather_ui("request_weather_ui_1")

## To be copied in the server
# callModule(mod_reqeust_weather_server, "request_weather_server_1")

