#' track_requests UI Function
#'
#' @description A shiny Module.
#'
#' @param id,input,output,session Internal parameters for {shiny}.
#'
#' @noRd 
#'
#' @importFrom shiny NS tagList 
mod_track_requests_ui <- function(id){
  ns <- NS(id)
  tagList(
    sidebarLayout(
      sidebarPanel(
        width = 4,
        
        searchInput(
          inputId = ns("tr_email"),
          label = h4("Enter email to track requests"), 
          placeholder = "me@email.com",
          btnSearch = icon("search"), 
          width = "100%"
        ),
        
        tags$br(),
        
        p(
          paste0("You will be able to able to download all finished jobs",
                 "by selecting JOB ID below. Please first provide your email ",
                 "in the box above to proceed.")
        ),
        
        tags$br(),
        
        uiOutput(ns("tr_slider")),
        
        tags$br(),
        
        disabled(downloadButton(ns("tr_download"), "Download Data"))
        
      ),
      mainPanel(
        h3("Jobs Submitted"),
        tags$br(),
        tableOutput(ns("tr_view"))
      )
    )
    
  )
}

#' track_requests Server Function
#'
#' @noRd 
mod_track_requests_server <- function(input, output, session){
  ns <- session$ns
  final_zip <- NULL
  work_dir <- "/w3s_jobs/"
  
  output$tr_slider <- renderUI({
    disabled(
      pickerInput(
        inputId = ns("tr_selected_job"),
        label = h4("Select Job ID to download"), 
        choices = NULL,
        options = list(title = "No Job IDs available"))
    )
  })
  
  # When email address is entered
  observeEvent(input$tr_email_search,{
    my_requests <- get_requests(input$tr_email)
    my_requests$sub_time <- format(my_requests$sub_time, "%Y-%b-%d %H:%M:%S")
    my_requests$fin_time <- format(my_requests$fin_time, "%Y-%b-%d %H:%M:%S")
    print(my_requests)
    output$tr_view <- renderTable({
      my_requests
    })
    
    my_finished_requests <- filter(my_requests, status == "finished")
    if(nrow(my_finished_requests) > 0){
      job_names <- my_finished_requests$job_id
      updatePickerInput(
        session = session,
        inputId = "tr_selected_job",
        choices = job_names,
        selected = job_names[1]
      )
      shinyjs::enable("tr_slider")
      shinyjs::enable("tr_download")
      final_zip <<- paste(work_dir, job_names[1], "/historicalData.zip", sep="")
    }
    else{
      updatePickerInput(
        session = session,
        inputId = "tr_selected_job",
        choices = NULL
      )
      shinyjs::disable("tr_slider")
      shinyjs::disable("tr_download")
    }
    
    
    # Update file to download based on user selection
    observeEvent(input$tr_selected_job,{
      final_zip <<- paste(work_dir, input$tr_selected_job,
                          "/historicalData.zip", sep="")
    })
    
    # Downloadable selected climate change data ----
    output$tr_download <- downloadHandler(
      filename = function(){
        paste("historicalData", "zip", sep=".")
      },
      content = function(file) {
        shinyjs::disable("hist_downloadClimate")
        file.copy(final_zip, file)
      },
      contentType = "application/zip"
    )
    
    
  })
  
  
}

## To be copied in the UI
# mod_track_requests_ui("track_requests_ui_1")

## To be copied in the server
# callModule(mod_track_requests_server, "track_requests_ui_1")

