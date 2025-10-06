#' upload_shape UI Function
#'
#' @description A shiny Module.
#'
#' @param id,input,output,session Internal parameters for {shiny}.
#'
#' @noRd 
#'
#' @importFrom shiny NS tagList 
mod_upload_shape_ui <- function(id){
  ns <- NS(id)
  tagList(
    fileInput(ns("shpFile"),
              "Upload Zipped Boundary Shape File",
              accept = ".zip",
              multiple = FALSE)
  )
}

#' upload_shape Server Function
#'
#' @noRd 
mod_upload_shape_server <- function(input, output, session){
  ns <- session$ns
  
  rv <- reactiveValues(shape=NULL)
  
  observeEvent(input$shpFile, {
    
    inFile <- input$shpFile
    # Once the filepath is known, copy file to server folder
    unlink("./Scratch", recursive = TRUE)
    dir.create("Scratch")
    unzip(inFile$datapath, exdir = "./Scratch",  junkpaths = TRUE)
    
    # Check shape file format and load shape file
    dbfFiles <- list.files(path="./Scratch", pattern = "\\.shp$")
    if (length(dbfFiles) == 0){
      sendSweetAlert(
        session = session,
        title = "Invalid upload",
        text = "No shape file found",
        type = "error"
      )
      unlink("./Scratch", recursive = TRUE)
    }
    else if (length(dbfFiles) > 1){
      sendSweetAlert(
        session = session,
        title = "Multiple shapes found",
        text = "Choosing first file.",
        type = "warning"
      )
      shpName <- paste("Scratch/", dbfFiles[1], sep = "")
      rv$shape <- st_read(shpName, crs = 4326)
    }
    else {
      shpName <- paste("Scratch/", dbfFiles[1], sep = "")
      rv$shape <- st_read(shpName, crs = 4326)
    }
    
    # Display shapefile on map
    if (length(rv$shape) > 1){ rv$shape <- rv$shape[1]}
    
  })
  
  returnList <- reactive({
    print(rv$shape)
    
  })
  
  return(list(
    reactive({ shape = rv$shape })
  ))
  
}

## To be copied in the UI
# mod_upload_shape_ui("upload_shape_ui_1")

## To be copied in the server
# callModule(mod_upload_shape_server, "upload_shape_ui_1")

