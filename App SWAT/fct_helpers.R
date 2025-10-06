
create_job_id <- function(){
  # Step 1 --- Create unique jobId
  d <- as.POSIXct(Sys.time())
  jobId = format(d, format = "%Y_%b_%d_%H_%M_%OS3")
  con <- dbConnect(RPostgres::Postgres(), dbname='postgres',
                   host='172.17.0.1', port=6543, user='postgres',
                   password='Drkupi2019!')
  req_tbl <- DBI::Id(schema = "world_climate",
                     table = "requests")
  if (dbExistsTable(con, req_tbl)){
    jobList <- dbReadTable(con, req_tbl)
    if (jobId %in% jobList$jobId){
      i = 0
      newId = paste(jobId, "_", i)
      while (newId %in% jobList$jobId){
        newId = paste(jobId, "_", i+1)
        i = i+1
      }
      jobId = newId
    }
  }
  dbDisconnect(con)
  return(jobId)
}

submit_job <- function(jobId, jobParams, selArea){
  
  
  # Step 2 ---- Prepare request data row
  selArea$job_id <- jobId
  selArea$sub_time <- as.POSIXct(Sys.time())
  selArea$status <- "pending"
  selArea$fin_time <- NA
  selArea$email <- jobParams$email
  selArea$start_date <- jobParams$start_date
  selArea$end_date <- jobParams$end_date
  selArea$data_type <- jobParams$data_type
  selArea$data_format <- jobParams$data_format
  selArea$var_flag <- jobParams$var_flag
  
  request <- selArea %>%
    select(job_id, sub_time, status, fin_time,
           email, start_date, end_date, data_type,
           data_format, var_flag, geometry) %>%
    rename(geom = geometry)
  
  # Step 3 ----- Write data to DB
  con <- dbConnect(RPostgres::Postgres(), dbname='postgres',
                   host='172.17.0.1', port=6543, user='postgres',
                   password='Drkupi2019!')
  req_tbl <- DBI::Id(schema = "world_climate",
                     table = "requests")
  if (dbExistsTable(con, req_tbl)){
    dbWriteTable(con, req_tbl, request,  append = TRUE)
  }
  else {
    dbWriteTable(con, req_tbl, request,
                 field.types = c(sub_time='timestamp',
                                 fin_time='timestamp', geom='geometry'))
  }
  dbDisconnect(con)
  
}


send_submission_email <- function(jobId, email_address){
  
  # Compose the email message
  email_body = md(c(
    "Hello!!!
  
  Your request for climate data from the W3S platform has been successfully
  submitted. You can check and monitor the status of the submitted request on the
  Track Requests tab on the W3S platform (https://www.uoguelph.ca/watershed/w3s/).
  Please use your email to check the status of all pending requests you submitted.
  Once your request is processed, we will send you an email confirming its completion.
  You may download the requested data from the above link then. Kindly use either your
  email or your jobId for retrieving the requested data. Your jobId is ", jobId, " Please contact us for further
  information.
  
  Best regards,
  
  W3S Team
  "))
  
  footer = md(
    "The W3S platform is developed by the watershed group at UoGuelph Canada."
  )
  
  email <- compose_email(
    body = email_body,
    footer = footer
  )
  
  # Send the email message
  status <- try({
    email %>%
      smtp_send(
        from = "akhtart@uoguelph.ca",
        to = email_address,
        subject = paste0("W3S Climate Data Request Submitted - jobId: ", jobId),
        credentials = creds_file("uog_creds")
      )
  })
  
  return(status)
  
}

get_requests <- function(email){
  
  # Step 1 --- Create unique jobId
  con <- dbConnect(RPostgres::Postgres(), dbname='postgres',
                   host='localhost', port=6543, user='postgres',
                   password='Drkupi2019!')
  
  # con <- dbConnect(RPostgres::Postgres(), dbname='postgres',
  #                  host='172.17.0.1', port=6543, user='postgres',
  #                  password='Drkupi2019!')
  
  query = paste0("SELECT job_id, sub_time, status, fin_time ",
                 "FROM world_climate.requests AS requests ",
                 "WHERE email = '", email, "'")
  
  req_tbl <- DBI::Id(schema = "world_climate",
                     table = "requests")
  if (dbExistsTable(con, req_tbl)){
    jobList <- dbGetQuery(con, query)
  }
  dbDisconnect(con)
  return(jobList)
}


