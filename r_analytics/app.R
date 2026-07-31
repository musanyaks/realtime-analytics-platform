# =============================================================================
# Real-Time Analytics Dashboard (Shiny)
# =============================================================================

library(shiny)
library(shinydashboard)
library(plotly)
library(DT)
library(dplyr)
library(DBI)
library(odbc)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT   <- Sys.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      <- Sys.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  <- Sys.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  <- Sys.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DB")
SNOWFLAKE_SCHEMA    <- Sys.getenv("SNOWFLAKE_SCHEMA", "PROD")
SNOWFLAKE_WAREHOUSE <- Sys.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

# ---------------------------------------------------------------------------
# Database Connection
# ---------------------------------------------------------------------------
get_connection <- function() {
  dbConnect(odbc::odbc(),
            Driver    = "Snowflake",
            Server    = paste0(SNOWFLAKE_ACCOUNT, ".snowflakecomputing.com"),
            UID       = SNOWFLAKE_USER,
            PWD       = SNOWFLAKE_PASSWORD,
            Database  = SNOWFLAKE_DATABASE,
            Schema    = SNOWFLAKE_SCHEMA,
            Warehouse = SNOWFLAKE_WAREHOUSE,
            Role      = "ACCOUNTADMIN"
  )
}

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
ui <- dashboardPage(
  dashboardHeader(title = "Real-Time Analytics"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem("Overview", tabName = "overview", icon = icon("dashboard")),
      menuItem("Hourly Metrics", tabName = "hourly", icon = icon("clock")),
      menuItem("User Insights", tabName = "users", icon = icon("users")),
      menuItem("Data Quality", tabName = "quality", icon = icon("check-circle"))
    ),
    actionButton("refresh", "Refresh Data", icon = icon("sync"), 
                 style = "margin: 15px; width: 80%;")
  ),
  
  dashboardBody(
    tags$head(tags$style(HTML("
      .content-wrapper { background-color: #1e1e2f; }
      .box { background: #27293d; border: 1px solid #444; }
      .box-header { color: #fff; }
    "))),
    
    tabItems(
      # Overview Tab
      tabItem(tabName = "overview",
              fluidRow(
                valueBoxOutput("total_events_box", width = 3),
                valueBoxOutput("revenue_box", width = 3),
                valueBoxOutput("users_box", width = 3),
                valueBoxOutput("conversion_box", width = 3)
              ),
              fluidRow(
                box(plotlyOutput("events_trend"), width = 8, title = "Events Trend (24h)"),
                box(plotlyOutput("device_breakdown"), width = 4, title = "Device Breakdown")
              )
      ),
      
      # Hourly Metrics Tab
      tabItem(tabName = "hourly",
              fluidRow(
                box(DTOutput("hourly_table"), width = 12, title = "Hourly Performance")
              ),
              fluidRow(
                box(plotlyOutput("hourly_revenue"), width = 6, title = "Revenue by Hour"),
                box(plotlyOutput("hourly_conversion"), width = 6, title = "Conversion by Hour")
              )
      ),
      
      # Users Tab
      tabItem(tabName = "users",
              fluidRow(
                box(plotlyOutput("user_segments"), width = 6, title = "User Segments"),
                box(plotlyOutput("top_countries"), width = 6, title = "Top Countries")
              ),
              fluidRow(
                box(DTOutput("top_users_table"), width = 12, title = "Top Users by LTV")
              )
      ),
      
      # Data Quality Tab
      tabItem(tabName = "quality",
              fluidRow(
                box(DTOutput("pipeline_status"), width = 12, title = "Pipeline Monitoring")
              )
      )
    )
  )
)

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------
server <- function(input, output, session) {
  
  data <- reactiveVal(NULL)
  
  load_data <- function() {
    con <- get_connection()
    on.exit(dbDisconnect(con))
    
    hourly <- dbGetQuery(con, "
      SELECT * FROM PROD.FCT_HOURLY_METRICS 
      WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
      ORDER BY event_date DESC, event_hour DESC
      LIMIT 168
    ")
    
    users <- dbGetQuery(con, "
      SELECT * FROM PROD.DIM_USERS 
      ORDER BY lifetime_value DESC 
      LIMIT 100
    ")
    
    monitoring <- dbGetQuery(con, "SELECT * FROM RAW.PIPELINE_MONITORING")
    
    list(hourly = hourly, users = users, monitoring = monitoring)
  }
  
  observe({
    data(load_data())
  })
  
  observeEvent(input$refresh, {
    data(load_data())
  })
  
  # Auto-refresh every 60 seconds
  autoInvalidate <- reactiveTimer(60000)
  observe({
    autoInvalidate()
    data(load_data())
  })
  
  # Value Boxes
  output$total_events_box <- renderValueBox({
    req(data())
    total <- sum(data()$hourly$total_events, na.rm = TRUE)
    valueBox(
      value = scales::comma(total),
      subtitle = "Total Events (7d)",
      icon = icon("bolt"),
      color = "yellow"
    )
  })
  
  output$revenue_box <- renderValueBox({
    req(data())
    rev <- sum(data()$hourly$revenue, na.rm = TRUE)
    valueBox(
      value = scales::dollar(rev),
      subtitle = "Revenue (7d)",
      icon = icon("dollar-sign"),
      color = "green"
    )
  })
  
  output$users_box <- renderValueBox({
    req(data())
    users <- sum(data()$hourly$unique_users, na.rm = TRUE)
    valueBox(
      value = scales::comma(users),
      subtitle = "Unique Users (7d)",
      icon = icon("users"),
      color = "aqua"
    )
  })
  
  output$conversion_box <- renderValueBox({
    req(data())
    conv <- weighted.mean(data()$hourly$conversion_rate, 
                          data()$hourly$unique_sessions, na.rm = TRUE)
    valueBox(
      value = scales::percent(conv, accuracy = 0.1),
      subtitle = "Avg Conversion",
      icon = icon("chart-line"),
      color = "purple"
    )
  })
  
  # Plots
  output$events_trend <- renderPlotly({
    req(data())
    p <- data()$hourly %>%
      mutate(datetime = as.POSIXct(paste(event_date, sprintf("%02d:00:00", event_hour)))) %>%
      arrange(datetime) %>%
      plot_ly(x = ~datetime, y = ~total_events, type = 'scatter', mode = 'lines',
              line = list(color = '#00d2ff'), fill = 'tozeroy',
              fillcolor = 'rgba(0, 210, 255, 0.2)') %>%
      layout(paper_bgcolor = '#27293d', plot_bgcolor = '#27293d',
             font = list(color = '#fff'),
             xaxis = list(title = "", gridcolor = '#444'),
             yaxis = list(title = "Events", gridcolor = '#444'))
    p
  })
  
  output$device_breakdown <- renderPlotly({
    req(data())
    p <- data()$hourly %>%
      group_by(event_date) %>%
      summarise(mobile = mean(mobile_share, na.rm = TRUE), .groups = "drop") %>%
      plot_ly(labels = ~c("Mobile", "Desktop/Tablet"), 
              values = ~c(mean(mobile), 1 - mean(mobile)),
              type = 'pie',
              marker = list(colors = c('#00d2ff', '#ff6b6b'))) %>%
      layout(paper_bgcolor = '#27293d', font = list(color = '#fff'),
             showlegend = FALSE)
    p
  })
  
  output$hourly_table <- renderDT({
    req(data())
    datatable(data()$hourly, options = list(pageLength = 24), 
              class = 'cell-border stripe')
  })
  
  output$hourly_revenue <- renderPlotly({
    req(data())
    p <- data()$hourly %>%
      plot_ly(x = ~event_hour, y = ~revenue, type = 'bar',
              marker = list(color = '#00d2ff')) %>%
      layout(paper_bgcolor = '#27293d', plot_bgcolor = '#27293d',
             font = list(color = '#fff'),
             xaxis = list(title = "Hour", gridcolor = '#444'),
             yaxis = list(title = "Revenue", gridcolor = '#444'))
    p
  })
  
  output$hourly_conversion <- renderPlotly({
    req(data())
    p <- data()$hourly %>%
      plot_ly(x = ~event_hour, y = ~conversion_rate, type = 'bar',
              marker = list(color = '#ff6b6b')) %>%
      layout(paper_bgcolor = '#27293d', plot_bgcolor = '#27293d',
             font = list(color = '#fff'),
             xaxis = list(title = "Hour", gridcolor = '#444'),
             yaxis = list(title = "Conversion Rate", gridcolor = '#444'))
    p
  })
  
  output$user_segments <- renderPlotly({
    req(data())
    p <- data()$users %>%
      count(user_segment) %>%
      plot_ly(x = ~user_segment, y = ~n, type = 'bar',
              marker = list(color = c('#00d2ff', '#ff6b6b', '#feca57', '#48dbfb'))) %>%
      layout(paper_bgcolor = '#27293d', plot_bgcolor = '#27293d',
             font = list(color = '#fff'))
    p
  })
  
  output$top_countries <- renderPlotly({
    req(data())
    p <- data()$users %>%
      count(most_common_country) %>%
      arrange(desc(n)) %>%
      head(10) %>%
      plot_ly(x = ~most_common_country, y = ~n, type = 'bar',
              marker = list(color = '#feca57')) %>%
      layout(paper_bgcolor = '#27293d', plot_bgcolor = '#27293d',
             font = list(color = '#fff'))
    p
  })
  
  output$top_users_table <- renderDT({
    req(data())
    datatable(data()$users, options = list(pageLength = 10))
  })
  
  output$pipeline_status <- renderDT({
    req(data())
    datatable(data()$monitoring, options = list(pageLength = 10))
  })
}

shinyApp(ui, server)