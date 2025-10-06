library(shiny)
library(tidyverse)
library(lubridate)
library(signal)
library(magrittr)
library(zoo)
library(xts)
library(dygraphs)
library(readxl)
library(tools)

ui <- fluidPage(
  
  h2('Plot hydrographs interactively'),
  
  sidebarLayout(
    sidebarPanel(
      fileInput('file', 'Choose info-file to upload',
                accept = c(
                  'text/csv',
                  'text/comma-separated-values',
                  'text/tab-separated-values',
                  'text/plain',
                  '.csv',
                  '.xlsx',
                  '.xls',
                  '.tsv'
                )
      ),
      helpText(".xslx, .xls, .csv, .txt and maybe more…"),
      # Taken from: http://shiny.rstudio.com/gallery/file-upload.html
      tags$hr(),
      checkboxInput('header', 'Header', TRUE),
      radioButtons('sep', 'Separator',
                   c(Comma=',',
                     Semicolon=';',
                     Tab='\t'),
                   ';'),
      radioButtons('quote', 'Quote',
                   c(None='',
                     'Double Quote'='"',
                     'Single Quote'="'"),
                   ''),
      radioButtons('dec', 'Decimal',
                   c(Comma = ',',
                     Dot = '.'),
                   ','),
      actionButton("choice", "Read File"),
      ################################################################
      
      # author info
      shiny::hr(),
      em(
        span("Created by "),
        a("Anatoly Tsyplenkov", href = "mailto:atsyplenkov@gmail.com"),
        span(", Apr 2020"),
        br(), br()
      )
    ),
    mainPanel(
      
      selectInput("date_column", "Select Date Column", choices = NULL), # no choices before uploading 
      # selectInput("bs_column", "Select Water Level Columns", choices = NULL), # no choices before uploading 
      checkboxGroupInput("bs_column", "Select Water Level Columns", choices = NULL, inline = T),
      
      tags$hr(),
      
      dygraphOutput("dygraph"),
      
      tags$hr(),
      
      helpText("Zoom: click-drag, Pan: shift-click-drag, Restore: double-click, Copy: click.")
    )
  )
)

server <- function(input, output, session) { # added session for updateSelectInput
  
  # Set the maximum file size
  options(shiny.maxRequestSize = 100*1024^2)
  
  info <- eventReactive(input$choice, {
    inFile <- input$file
    # Instead # if (is.null(inFile)) ... use "req"
    req(inFile)
    
    if (file_ext(inFile) %in% c("xlsx", "xls")) {
      
      f <- read_excel(inFile$datapath)
      
    } else {
      
      # Changes in read.table 
      f <- read.table(inFile$datapath,
                      header = input$header,
                      sep = input$sep,
                      quote = input$quote,
                      dec = input$dec,
                      fileEncoding = "utf8",
                      stringsAsFactors = F)
    }
    vars <- names(f)
    # Update select input immediately after clicking on the action button. 
    updateSelectInput(session, "date_column", "Select Date Column", choices = vars)
    updateCheckboxGroupInput(session, "bs_column", "Select Water Level Columns", choices = vars,
                             inline = T)
    
    f
  })
  
  Dataset <- reactive({
    
    req(info())
    
    f <- info()
    f <- dplyr::select(f,
                       input$date_column,
                       input$bs_column) %>%  #subsetting takes place here
      rename(datetime = 1) %>% 
      # magrittr::set_colnames(c("datetime", "bs")) %>% 
      mutate(datetime = lubridate::parse_date_time(datetime,
                                                   c("%d.%m.%Y %H:%M",
                                                     "%Y-%m-%d %H:%M:%S"),
                                                   tz = "Europe/Moscow"))
    
    f
    
  })
  
  output$dygraph <- renderDygraph({
    
    req(Dataset())
    
    df <- Dataset()
    
    df %>% 
      as.data.frame() %>% 
      xts(x = .[, -1], order.by = .$datetime) %>%
      dygraph() %>% 
      # dyAxis("y", label = "Уровень воды, мБС") %>% 
      dyRangeSelector() %>% 
      dyUnzoom() %>% 
      dyCrosshair(direction = "vertical")
  })
  
  
}
shinyApp(ui, server)
