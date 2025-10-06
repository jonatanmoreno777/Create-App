
#install.packages("chron")
#install.packages("RColorBrewer")
#install.packages("RNetCDF")
#install.packages("ncdf.tools")
#install.packages("nc")
#install.packages("lattice")

library(shiny)

library(chron)
library(RColorBrewer)
library(RNetCDF)
#library(ncdf.tools)
library(ncdf4)
library(lattice)

# creating user interface
ui <- fluidPage(
  
  titlePanel(h1("NetCDF4 File Extractor to SWAT Format",
                style="color: #ffff;background-color: #eb4034;border-color: #f27a72;text-align: center")),
  
  sidebarLayout(
    
    sidebarPanel(h2("Input data",
                    style="color: #ffff;background-color: #337ab7;border-color: #2e6da4;text-align: center"),
                 #textInput("Subbasin_No", "Enter number of stations: "),
                 numericInput("InTotal_1_St_data", "Enter Total number of data for one station: ",365),
                 numericInput("InSubbasin_No", "Enter number of stations: ",20),
                 numericInput("InStartDate", "Enter Start date in format YYYYMMDD (e.g. 20070101): ",20070101),
                 #textInput("InStartDate", "Enter Start date in format YYYYMMDD (e.g. 20070101): "),
                 textInput("InVariable_name", "Enter variable name of NetCDF (e.g. precipitation): ", "precipitation"),
                 actionButton("BtnExtract", "Extract Data in SWAT Format",
                              style="color: #fff;background-color: #fc6900;text-align: center"),
                 actionButton("BtnQuit", "Quit",
                              style="color: #fff;background-color: #00abfa;text-align: center"),
                 #p("Created by Ajay Yadav"),
                 br(),
                 textOutput("Message")),
    
    mainPanel(h3("Output Panel : Total Sum of data for each station",
                 style="color: #ffff;background-color: #42c954;border-color: #2e6da4;text-align: center"),
              
              tabsetPanel(
                tabPanel("Table",column(12,dataTableOutput('table'))),
                tabPanel("Plot", plotOutput("HistPlot")),
                tabPanel("About",
                         p("Created by Ajay Yadav"),
                         br(),
                         p("This is a free open-source program been built using R and Shiny"),
                         p("This Program is follow-up of CSAY TRMM Data Downloader written in C#"),
                         p("Download CSAY TRMM Data Downloader from my github account-"),
                         p(a(h4("Click here to Download CSAY TRMM DATA DOWNLOADER"), href="https://github.com/ajayyadavay/CSAYTRMMDATADOWNLOADER", target="_blank")),
                         p("From the above software, you will get detail about downloading link list file"),
                         p("and then using the above software. Then use this software to extract data")
                )
              )
              
    )
    #column(12,dataTableOutput('table'))
    #textOutput("Message"))
  )
)

# code 
server <- function(input, output, session) {
  
  # close the R session when Chrome closes
  session$onSessionEnded(function() { 
    stopApp()
    q("no") 
  })
  
  observeEvent(input$BtnQuit, {
    stopApp()
    
  })
  
  observeEvent(input$BtnExtract, {
    
    # taking input from the user and storing in  variables
    # output$Subbasin_No <- input$InSubbasin_No
    # output$StartDate <- input$InStartDate
    # output$Variable_name <- input$InVariable_name
    
    Total_1_St_data <- input$InTotal_1_St_data
    Subbasin_No <- input$InSubbasin_No
    StartDate <- input$InStartDate
    Variable_name <- input$InVariable_name
    
    #Main Program
    #choose any one file with *.nc4 to know variables like "precipitation",
    #variables like lat, long, etc
    #open_nc <-nc_open("3B42_Daily.20070101.7.nc4.nc4")
    open_nc  <-nc_open(choose.files(default = "", caption = "Select one *.nc4 file",
                                    multi = TRUE, filters = Filters,
                                    index = nrow(Filters)))
    open_nc 
    lat <-ncvar_get(open_nc ,'lat')
    lon <-ncvar_get(open_nc ,'lon')
    lat
    lon
    dim(lat)
    dim(lon)
    
    #Input Subbasin number i.e. number of data
    #Subbasin_No = 20
    #Input Folder containing all *.nc4 files
    #NetCDF4_Folder = "F:/AY/SWAT/Manual/TRMM/TRMM_2007"
    NetCDF4_Folder = choose.dir(default = "", caption = "Select folder containing all *.nc4")
    Output_Folder = choose.dir(default = "", caption = "Select Output Folder")
    #Input Variable name
    #Variable_name = "precipitation"
    #Input Start Date in Format YYYYMMDD e.g. 20070101
    #StartDate = 20070101
    
    # Read text file data into R
    my_data <- read.delim(choose.files(default = "", caption = "Select text file containing name,lat,long",
                                       multi = TRUE, filters = Filters,
                                       index = nrow(Filters)),
                          stringsAsFactor = FALSE)
    
    
    # Centroid_Lat <- c(30.55810413,	30.48717771,	30.39697899,	30.28824012,	30.2428854,	30.07583257,	30.11055243,	29.86128231,	29.85438017,	29.77012434,	29.76662837,	29.53797134,	29.57705944,	29.36444532,	29.27276843,	29.38234064,	29.16762209,	29.39878993,	29.10281023,	29.09877671)
    # Centroid_Lon <- c(80.76913331,	81.11928033,	80.92471719,	81.64901962,	81.2027328,	81.53405501,	82.03234314,	82.26980975,	81.89355133,	81.65894633,	82.53616381,	82.79000672,	82.15859984,	83.23267801,	82.39683407,	82.12944527,	82.1011236,	81.74724425,	81.8170532,	81.47019901)
    # Subbasin_Name <- c("pKAR01",	"pKAR02",	"pKAR03",	"pKAR04",	"pKAR05",	"pKAR06",	"pKAR07",	"pKAR08",	"pKAR10",	"pKAR11",	"pKAR12",	"pKAR13",	"pKAR14",	"pKAR16",	"pKAR20",	"pKAR21",	"pKAR22",	"pKAR26",	"pKAR27",	"pKAR31")
    
    Subbasin_Name <- my_data[,1] #assign first column as subbasin_name
    Centroid_Lat <- my_data[,2] #assign second column as latitude
    Centroid_Lon <- my_data[,3] #assign third column as longitude
    
    #Total_1_St_data <- 365 # total number of data in one station i.e. for two yeas, it will be 365+365=730
    St_wise_data <- array(dim = c(Total_1_St_data,Subbasin_No)) #this array stores stationwise data for all years
    ArrayData <- array(0, dim=c(Subbasin_No,1))   # matrix of size row=Subbasin_No, column=1 and all element values = 0
    df2 = NULL
    
    for(k in seq(1, Subbasin_No, by=1))
    {
      
      Lat_No <- which.min(abs(lat-Centroid_Lat[k]))
      Lon_No <- which.min(abs(lon-Centroid_Lon[k]))
      
      df=NULL
      files= list.files(NetCDF4_Folder,pattern="*.nc4",full.names=TRUE)
      for (i in seq_along(files))
      { 
        nc= open.nc (files[i]) 
        precip= var.get.nc(nc, Variable_name)
        x=dim(precip)
        precip= var.get.nc(nc,Variable_name,start= c(Lat_No,Lon_No),count=c(1,1))
        rbind(df,data.frame(precip)) -> df
        
        ArrayData[k,1] = ArrayData[k,1] + precip
        
        St_wise_data[i,k] = precip
        
      }
      
      rbind(df2, data.frame(ArrayData[k,1]))-> df2
      
      write.table(StartDate, file = paste(Output_Folder,"\\",Subbasin_Name[k], ".txt", sep = ""), append = TRUE, sep = "\t",row.names = FALSE, col.names = FALSE)
      write.table(df, file = paste(Output_Folder,"\\",Subbasin_Name[k], ".txt", sep = ""), append = TRUE, sep = "\t",row.names = FALSE, col.names = FALSE)
      
    }
    
    write.table(St_wise_data, file = paste(Output_Folder,"\\","AllData", ".csv", sep = ""), append = TRUE, sep = ",",row.names = FALSE, col.names = FALSE)
    
    output$table <- renderDataTable(df2)
    output$HistPlot <- renderPlot(
      
      #hist(unlist(df2), main = "Histogram of sum of data of each station", xlab = "Station", ylab = "Sum", breaks = seq(0,max(unlist(df)),l=Subbasin_No))
      barplot(unlist(df2),names.arg= Subbasin_Name ,xlab="station",ylab="Sum of Data",col="dodger blue",main="Chart of Sum of stationwise Data")
      #names.arg= Subbasin_Name  
    )
    
    output$Message <- renderText("Data Exported in SWAT Format (*.txt) and all data in one .csv")
  })
  
  
  
}

shinyApp(ui, server)

