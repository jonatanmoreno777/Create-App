library(leaflet)
library(mapview)
library(leaflet)
library(rgdal)
library(leaflet)
library(leaflet.extras)

# Leer los datos de los límites geográficos
boundary <- readOGR(dsn='D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/Country/', layer='CanadianGreatLakes')

# Crear etiquetas para los polígonos
pal <- colorBin("jet", domain = boundary$AREA)
labels <- sprintf(
  "<strong>Lake %s</strong><br/> Area: %f th. mi<sup>2</sup>",
  boundary$LAKEBASIN, boundary$AREA
) %>% lapply(htmltools::HTML)


# Definir los colores de los polígonos
boundary$col = c('red', 'blue', 'green', 'yellow')

# Crear el mapa base con Leaflet
basemap <- leaflet() %>%
  addTiles(
    urlTemplate = "//{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    attribution = 'Maps by <a href="http://www.openstreetmap.org">OpenStreetMap</a>'
  ) %>%
  # setView(-80, lat = 45, zoom=6) %>%
  # setMaxBounds(lng1 = -95
  #              , lat1 = 35
  #              , lng2 = -65
  #              , lat2 = 55 ) %>%
  setView(-80, lat = 45, zoom=6) %>%
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
    editOptions = editToolbarOptions(edit = FALSE, remove = TRUE, selectedPathOptions = selectedPathOptions())
  ) %>%
  addPolygons(
    data = boundary, color = "black", dashArray = "3", fillColor = boundary$col, weight = 2, smoothFactor = 0.5, opacity = 0.2,
    highlight = highlightOptions(weight = 5,
                                 color="red",
                                 fillOpacity = 0.4,
                                 dashArray = "",
                                 bringToFront = FALSE),
    label = labels,
    stroke = FALSE
  )

# Mostrar el mapa
basemap

# Leer los datos de los límites geográficos
library(leaflet)
library(rgdal)
library(htmltools)

# Leer los datos del Shapefile
boundary <- readOGR(dsn='D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/basin', layer='PeruHydrographicSlope')

# Crear etiquetas para los polígonos
pal <- colorBin("YlOrRd", domain = boundary$AREA)
labels <- sprintf(
  "Lake %s Area: %.2f th. mi^{2}",
  boundary$HIDROGRAPH, boundary$AREA
) %>% unlist()

# Definir los colores de los polígonos
boundary$col <- c('red', 'blue', 'green')

# Crear el mapa base con Leaflet
basemap <- leaflet(options = leafletOptions(backgroundColor = "white")) %>%
  addTiles(
    urlTemplate = "//{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    attribution = 'Maps by <a href="http://www.openstreetmap.org">OpenStreetMap</a>'
  ) %>%
  setView(lng = -73, lat = -10, zoom = 4.5) %>%
  addScaleBar(position = "bottomleft") %>%
  addDrawToolbar(
    targetGroup='Selected',
    polylineOptions=FALSE,
    markerOptions = FALSE,
    polygonOptions = drawPolygonOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    rectangleOptions = drawRectangleOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    circleOptions = FALSE,
    circleMarkerOptions = FALSE,
    singleFeature = TRUE,
    editOptions = editToolbarOptions(edit = T, remove = TRUE, selectedPathOptions = selectedPathOptions())
    
  ) %>%
  addPolygons(
    data = boundary, color = "black", dashArray = "3", fillColor = boundary$col,  weight = 1, smoothFactor = 0.5, opacity = 0.7,highlight = highlightOptions(
      weight = .5,
      color = "red",
      fillOpacity = 0.4,
      dashArray = "",
      bringToFront = FALSE
    ),
    label = labels,
    stroke = TRUE
  ) %>%
  addMiniMap(
    tiles = providers$OpenStreetMap,
    position = "bottomleft", # Posición en la parte inferior izquierda
    width = 100, # Ancho del minimapa
    height = 100 # Altura del minimapa
  ) %>%
    addLayersControl(
    baseGroups = c("Esri", "OpenStreetMap"),
    options = layersControlOptions(collapsed = TRUE)
  ) %>%
    addProviderTiles("Esri.WorldImagery", group = "Esri") %>%
    addProviderTiles("OpenStreetMap.Mapnik", group = "OpenStreetMap")

  
  # Mostrar el mapa
basemap


# Ruta del archivo CSS
ruta_css <- "D:/Paper_Climate/Data/siguiente paper/App/app2/styles.css"

# Abrir el archivo CSS en el editor predeterminado
file.edit(ruta_css)

# Leer el contenido del archivo modificado
nuevo_contenido <- readLines(ruta_css)

# Puedes realizar algunas modificaciones al contenido aquí si lo deseas

# Guardar el archivo con las modificaciones
writeLines(nuevo_contenido, ruta_css)

# Leer los datos del Shapefile


# Cargar datos del shapefile
library(leaflet)
library(leaflet.extras)

# Leer datos de polígonos
boundary <- readOGR(dsn='D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/basin', layer='PeruHydrographicSlope')

# Crear etiquetas para los polígonos
pal <- colorBin("YlOrRd", domain = boundary$AREA)
labels <- sprintf(
  "Lake %s Area: %.2f th. mi^{2}",
  boundary$HIDROGRAPH, boundary$AREA
) %>% unlist()

# Definir los colores de los polígonos
boundary$col <- c('red', 'blue', 'green')

# Crear el mapa base con Leaflet
basemap <- leaflet(options = leafletOptions(backgroundColor = "white")) %>%
  addProviderTiles("Esri.WorldImagery", group = "Esri") %>%
  addProviderTiles("OpenStreetMap.Mapnik", group = "OpenStreetMap") %>%
  setView(lng = -73, lat = -10, zoom = 4.5) %>%
  addScaleBar(position = "bottomleft") %>%
  addDrawToolbar(
    targetGroup='Selected',
    polylineOptions=FALSE,
    markerOptions = FALSE,
    polygonOptions = drawPolygonOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    rectangleOptions = drawRectangleOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    circleOptions = FALSE,
    circleMarkerOptions = FALSE,
    singleFeature = TRUE,
    editOptions = editToolbarOptions(edit = FALSE, remove = TRUE, selectedPathOptions = selectedPathOptions())
  ) %>%
  addPolygons(
    data = boundary, color = "black", dashArray = "3", fillColor = boundary$col,  weight = 1, smoothFactor = 0.5, opacity = 0.7,
    label = labels,
    stroke = TRUE,
    labelOptions = labelOptions(noHide = TRUE, textOnly = TRUE, direction = "auto"),
    popup = paste("Lake:", boundary$HIDROGRAPH, "<br>",
                  "Area:", boundary$AREA, "th. mi^{2}")
  ) %>%
  addLayersControl(
    baseGroups = c("Esri", "OpenStreetMap"),
    options = layersControlOptions(collapsed = TRUE),
    overlayGroups = c("Selected"),
    position = "bottomleft"
  ) 

basemap

# Instalar y cargar los paquetes necesarios
# Cargar las bibliotecas necesarias
library(gstat)
library(sp)

# Leer los datos de coordenadas de los puntos
coordenadas <- read.csv("D:/Paper_Climate/Data/siguiente paper/App/app2/pcp_tmp/wind_Data_1950_to_2015/wind.txt")

# Definir la resolución deseada (10 km)
resolucion <- 10  # En kilómetros

# Crear una nueva cuadrícula de 10 km
nueva_cuadricula <- expand.grid(
  LONG = seq(min(coordenadas$LONG), max(coordenadas$LONG), by = resolucion),
  LAT = seq(min(coordenadas$LAT), max(coordenadas$LAT), by = resolucion)
)

# Convertir la cuadrícula en un objeto de clase SpatialPoints
nueva_cuadricula_sp <- SpatialPoints(nueva_cuadricula, proj4string = CRS("+proj=longlat +datum=WGS84"))

# Inicializar una lista para almacenar las series temporales interpoladas
series_temporales <- list()

# Iterar sobre los archivos de datos de los puntos
for (i in 1:nrow(coordenadas)) {
  # Construir la ruta al archivo de datos del punto actual
  archivo <- paste("D:/Paper_Climate/Data/siguiente paper/App/app2/pcp_tmp/wind_Data_1950_to_2015/wind", coordenadas$LONG[i], "_", coordenadas$LAT[i], ".txt", sep = "")
  
  # Leer los datos del punto actual
  datos <- read.table(archivo, header = FALSE)
  colnames(datos) <- c("Date", "wint")
  datos$Date <- as.Date(as.character(datos$Date), format = "%Y%m%d")
  
  # Realizar la interpolación por vecinos más cercanos para el punto actual
  interpolacion <- idw(wint ~ 1, datos, nueva_cuadricula_sp)
  
  # Agregar las series temporales interpoladas a la lista
  series_temporales[[i]] <- data.frame(Date = datos$Date, wint_interpolado = interpolacion$var1.pred)
}

# Combinar todas las series temporales en un único marco de datos
serie_temporal_final <- do.call(rbind, series_temporales)

# Guardar los resultados en un nuevo archivo CSV
write.csv(serie_temporal_final, "D:/Paper_Climate/Data/siguiente paper/App/app2/pcp_tmp/wind_Data_1950_to_2015/wind_interpolado.csv", row.names = FALSE)




archivo <- paste("D:/Paper_Climate/Data/siguiente paper/App/app2/pcp_tmp/wind_Data_1950_to_2015/wind", coordenadas$LONG[i], "_", coordenadas$LAT[i], ".txt", sep = "")


datos <- read.table(archivo, header = FALSE)
colnames(datos) <- c("Date", "wint")
datos$Date <- as.Date(as.character(datos$Date), format = "%Y%m%d")

# Realizar la interpolación por vecinos más cercanos para el punto actual
interpolacion <- idw(wint ~ 1, datos, nueva_cuadricula_sp)

# Agregar las series temporales interpoladas a la lista
series_temporales[[i]] <- data.frame(Date = datos$Date, wint_interpolado = interpolacion$var1.pred)


# Combinar todas las series temporales en un único marco de datos
serie_temporal_final <- do.call(rbind, series_temporales)

library(leaflet)
library(htmltools)
library(rgdal)

# Definir el objeto 'boundary' con datos geoespaciales
boundary <- readOGR(dsn='D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/basin', layer='PeruHydrographicSlope')

# Crear etiquetas para los polígonos
pal <- colorBin("YlOrRd", domain = boundary$AREA)
labels <- sprintf(
  "Lake %s Area: %.2f th. mi^{2}",
  boundary$HIDROGRAPH, boundary$AREA
) %>% unlist()

# Definir los colores de los polígonos
boundary$col <- c('red', 'blue', 'green')

# Crear el mapa base con Leaflet
basemap <- leaflet(options = leafletOptions(backgroundColor = "white")) %>%
  addTiles(
    urlTemplate = "//{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    attribution = 'Maps by <a href="http://www.openstreetmap.org">OpenStreetMap</a>'
  ) %>%
  setView(lng = -73, lat = -10, zoom = 4.5) %>%
  addScaleBar(position = "bottomleft") %>%
  addDrawToolbar(
    targetGroup='Selected',
    polylineOptions=FALSE,
    markerOptions = FALSE,
    polygonOptions = drawPolygonOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    rectangleOptions = drawRectangleOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    circleOptions = FALSE,
    circleMarkerOptions = FALSE,
    singleFeature = TRUE,
    editOptions = editToolbarOptions(edit = TRUE, remove = TRUE, selectedPathOptions = selectedPathOptions())
    
  ) %>%
  addPolygons(
    data = boundary, color = "black", dashArray = "3", fillColor = boundary$col,  weight = 1, smoothFactor = 0.5, opacity = 0.7,highlight = highlightOptions(
      weight = .5,
      color = "red",
      fillOpacity = 0.4,
      dashArray = "",
      bringToFront = FALSE
    ),
    label = labels,
    stroke = TRUE
  ) %>%
  addMiniMap(
    tiles = providers$OpenStreetMap,
    position = "bottomleft", # Posición en la parte inferior izquierda
    width = 100, # Ancho del minimapa
    height = 100 # Altura del minimapa
  ) %>%
  addLayersControl(
    baseGroups = c("Esri", "OpenStreetMap"),
    options = layersControlOptions(collapsed = TRUE)
  ) %>%
  addProviderTiles("Esri.WorldImagery", group = "Esri") %>%
  addProviderTiles("OpenStreetMap.Mapnik", group = "OpenStreetMap")

# Mostrar el mapa
basemap


# Crear el mapa base con Leaflet
# Crear el mapa base con Leaflet
# Instalar y cargar el paquete htmltools si no está instalado
library(leaflet)
library(magrittr)  # Se agrega la carga del paquete magrittr
library(htmltools)

# Crear el mapa base con Leaflet
basemap <- leaflet(options = leafletOptions(backgroundColor = "white")) %>%
  addTiles(
    urlTemplate = "//{s}.tiles.mapbox.com/v3/jcheng.map-5ebohr46/{z}/{x}/{y}.png",
    attribution = 'Maps by <a href="http://www.mapbox.com/">Mapbox</a>'
  ) %>%
  setView(lng = -73, lat = -10, zoom = 4.5) %>%
  addScaleBar(position = "bottomleft") %>%
  addDrawToolbar(
    targetGroup='Selected',
    polylineOptions=FALSE,
    markerOptions = FALSE,
    polygonOptions = drawPolygonOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    rectangleOptions = drawRectangleOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    circleOptions = FALSE,
    circleMarkerOptions = FALSE,
    singleFeature = TRUE,
    editOptions = editToolbarOptions(edit = T, remove = TRUE, selectedPathOptions = selectedPathOptions())
  ) %>%
  addPolygons(
    data = boundary, color = "black", dashArray = "3", fillColor = boundary$col,  weight = 1, smoothFactor = 0.5, opacity = 0.7,highlight = highlightOptions(
      weight = .5,
      color = "red",
      fillOpacity = 0.4,
      dashArray = "",
      bringToFront = FALSE
    ),
    label = labels,
    stroke = TRUE
  ) %>%
  addMiniMap(
    tiles = providers$OpenStreetMap,
    position = "bottomleft", # Posición en la parte inferior izquierda
    width = 100, # Ancho del minimapa
    height = 100 # Altura del minimapa
  ) %>%
  addLayersControl(
    baseGroups = c("Esri", "OpenStreetMap"),
    options = layersControlOptions(collapsed = TRUE)
  ) %>%
  addProviderTiles("Esri.WorldImagery", group = "Esri") %>%
  addProviderTiles("OpenStreetMap.Mapnik", group = "OpenStreetMap") %>%
  addFullscreenControl() %>% # Agregar control de pantalla completa
  addEasyButton(easyButton(
    icon = "ion-android-locate", # Icono para recetear
    title = "Reset Map View", # Texto que aparece al pasar el ratón sobre el botón
    onClick = JS("function(btn, map){ map.setView([-73, -10], 4.5); }") # Función para recetear la vista del mapa
  ))

# Agregar título al mapa
basemap$title <- tags$h3("Mapa de Cuencas Hidrográficas de Perú")

# Agregar mensaje informativo
basemap$message <- tags$p("Haz clic en una cuenca para obtener más información.")

# Estilo adicional para las etiquetas
basemap %>% 
  addControl(html = '<style>.leaflet-label {font-family: Arial, sans-serif; font-size: 12px;}</style>', 
             position = 'bottomleft')

# Mostrar el mapa
basemap

######################################################################################################################
#######################################        okk

library(leaflet)
library(mapview)
library(leaflet)
library(rgdal)
library(leaflet)
library(leaflet.extras)
library(leaflet)
library(leafem)

# Leer el primer shapefile
boundary <- readOGR(dsn = 'D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/basin', layer = 'PeruHydrographicSlope')

# Crear etiquetas para los polígonos del primer shapefile
pal <- colorBin("YlOrRd", domain = boundary$AREA)
labels <- sprintf(
  "Lake %s Area: %.2f th. mi^{2}",
  boundary$HIDROGRAPH, boundary$AREA
) %>% unlist()

# Definir los colores de los polígonos del primer shapefile
boundary$col <- c('red', 'blue', 'green')

# Leer el segundo shapefile
otro_boundary <- readOGR(dsn = "D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/basin", layer = "South America")

# Crear un esquema de color para el segundo shapefile
pal_otro <- colorBin("BuPu", domain = otro_boundary$AREAKM2)
labels_otro <- sprintf(
  "Lake %s Area: %.2f th. mi^{2}",
  otro_boundary$Pais, otro_boundary$AREAKM2
) %>% unlist()


# Definir los colores de los polígonos del segundo shapefile
otro_boundary$col <- c('yellow', 'purple', 'orange', 'green',
                       'blue', 'red', 'pink', 'cyan', 'magenta', 
                       'brown', 'gray', 'darkgreen', 'darkblue',"tomato3") # Cambia los colores según tu preferencia

# Crear el mapa base con Leaflet
# Crear el mapa base con Leaflet
basemap <- leaflet(options = leafletOptions(backgroundColor = "white")) %>%
  addTiles(
    urlTemplate = "//{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    attribution = 'Maps by <a href="http://www.mapbox.com/">Mapbox</a>'
    #urlTemplate = "//{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
    #attribution = 'Maps by <a href="https://carto.com/">CartoDB</a>'
  ) %>%
  setView(lng = -66, lat = -27, zoom = 2.5) %>%
  addScaleBar(position = "bottomleft") %>%
  addDrawToolbar(
    targetGroup='Selected',
    polylineOptions=FALSE,
    markerOptions = FALSE,
    polygonOptions = drawPolygonOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    rectangleOptions = drawRectangleOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    circleOptions = FALSE,
    circleMarkerOptions = FALSE,
    singleFeature = TRUE,
    editOptions = editToolbarOptions(edit = F, remove = TRUE, selectedPathOptions = selectedPathOptions())
  ) %>%
  addPolygons(
    data = boundary,
    color = "black",
    dashArray = "3",
    fillColor = boundary$col,
    weight = 1,
    smoothFactor = 0.5,
    opacity = 0.7,
    highlight = highlightOptions(
      weight = .5,
      color = "red",
      fillOpacity = 0.4,
      dashArray = "",
      bringToFront = FALSE
    ),
    label = labels,
    stroke = TRUE,
    group = "Boundary"
  ) %>%
  addPolygons(
    data = otro_boundary,
    color = "black",
    dashArray = "3",
    fillColor = otro_boundary$col,
    weight = 1,
    smoothFactor = 0.5,
    opacity = 0.7,
    highlight = highlightOptions(
      weight = .5,
      color = "red",
      fillOpacity = 0.4,
      dashArray = "",
      bringToFront = FALSE
    ),
    label = labels_otro,
    stroke = TRUE,
    group = "otro_boundary"
  ) %>%
  addLayersControl(
    baseGroups = c("Esri", "OpenStreetMap", "OpenTopoMap", "CartoDB Positron"), # Agrega CartoDB Positron aquí
    overlayGroups = c("Boundary", "otro_boundary"),
    position = "topleft",
    options = layersControlOptions(collapsed = TRUE)
  ) %>%
  addProviderTiles("Esri.WorldImagery", group = "Esri") %>%
  addProviderTiles("OpenStreetMap.Mapnik", group = "OpenStreetMap") %>%
  addProviderTiles("OpenTopoMap", group = "OpenTopoMap") %>%
  addProviderTiles("CartoDB Positron", group = "CartoDB Positron") %>%
  leafem::addLogo( 
    "https://panda.maps.arcgis.com/sharing/rest/content/items/203af4ae947245948ad0958bc2c7ea9c/data",
    src = "remote",
    position = "bottomleft",
    offset.x = 10,
    offset.y = 200,
    width = 200,
    height = 40.5
  ) %>%
  
  addMouseCoordinates() %>% 
  setView(lng = -66, lat = -27, zoom = 2.5)

# Mostrar el mapa
basemap                 ########okkkkkkkkkkkkkkk

# Cargar la librería adicional

library(leaflet)
library(mapview)
library(leaflet)
library(rgdal)
library(leaflet)
library(leaflet.extras)
library(RColorBrewer)

#################################################################################################################################

boundary <- readOGR(dsn = 'D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/basin', layer = 'PeruHydrographicSlope')

# Crear etiquetas para los polígonos del primer shapefile
labels <- sprintf(
  "Lake %s Area: %.2f th. mi^{2}",
  boundary$HIDROGRAPH, boundary$AREA
) %>% unlist()

# Definir los colores de los polígonos del primer shapefile
boundary$col <- c('red', 'blue', 'green')

# Leer el segundo shapefile
otro_boundary <- readOGR(dsn = 'D:/Paper_Climate/Data/siguiente paper/App/app2/Basins/basin', layer = 'world')

# Crear un esquema de color para el segundo shapefile
pal_otro <- colorBin("BuPu", domain = otro_boundary$AREAKM2)
labels_otro <- sprintf(
  "Lake %s Area: %.2f th. mi^{2}",
  otro_boundary$NAME, otro_boundary$AREAKM2
) %>% unlist()


# Obtener el número de polígonos en otro_boundary
num_poligonos <- length(otro_boundary)

# Definir una paleta de colores basada en la paleta de colores "Spectral"
paleta <- colorRampPalette(brewer.pal(11, "Spectral"))

# Generar la secuencia de colores
colores <- paleta(num_poligonos)

# Asignar los colores al objeto otro_boundary
otro_boundary$col <- colores

# Crear el mapa base con Leaflet
# Crear el mapa base con Leaflet
basemap <- leaflet(options = leafletOptions(backgroundColor = "white")) %>%
  addTiles(
    urlTemplate = "//{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    attribution = 'Maps by <a href="http://www.openstreetmap.org">OpenStreetMap</a>'
  ) %>%
  setView(lng = -73, lat = -10, zoom = 4.5) %>%
  addScaleBar(position = "bottomleft") %>%
  addMouseCoordinates() %>% 
  addDrawToolbar(
    targetGroup='Selected',
    polylineOptions=FALSE,
    markerOptions = FALSE,
    polygonOptions = drawPolygonOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    rectangleOptions = drawRectangleOptions(shapeOptions=drawShapeOptions(fillOpacity = 0.3, color = 'black', weight = 1)),
    circleOptions = FALSE,
    circleMarkerOptions = FALSE,
    singleFeature = TRUE,
    editOptions = editToolbarOptions(edit = T, remove = TRUE, selectedPathOptions = selectedPathOptions())
  ) %>%
  addPolygons(
    data = boundary,
    color = "black",
    dashArray = "3",
    fillColor = boundary$col,
    weight = 1,
    smoothFactor = 0.5,
    opacity = 0.7,
    label = labels,
    stroke = TRUE,
    group = "Boundary" # Asignamos este grupo al primer shapefile
  ) %>%
  addPolygons(
    data = otro_boundary,
    color = "black",
    fillColor = otro_boundary$col,
    weight = 1,
    opacity = 0.7,
    label = labels_otro,
    group = "Otro Boundary" # Asignamos este grupo al segundo shapefile
  ) %>%
  addMiniMap(
    tiles = providers$OpenStreetMap,
    position = "bottomleft",
    width = 100,
    height = 100
  ) %>%
  addLayersControl(
    baseGroups = c("Esri", "OpenStreetMap"),
    overlayGroups = c("Boundary", "Otro Boundary"), # Agregamos los grupos para los shapefiles
    options = layersControlOptions(collapsed = TRUE)
  ) %>%
  addProviderTiles("Esri.WorldImagery", group = "Esri") %>%
  addProviderTiles("OpenStreetMap.Mapnik", group = "OpenStreetMap")

# Mostrar el mapa
basemap

library(tidyverse)
library(sf)
library(mapview)

#### Load data ----
existing_dams <- readRDS("D:/Hydrology/existing_dams_scenarios.rds") %>%
  mutate(CapacityCex = abs(sqrt(Capacity))/10) # for visualization purposes only


# Maps ----
mapviewOptions(legend.pos = "bottomleft")
mapviewOptions(fgb = FALSE)

my_col.regions <- colorRampPalette(scales::brewer_pal(type = "div", palette = "RdYlBu", direction = -1)(9))
legendBreaks <- c(-1.6, -1.2, -0.8, -0.4, -0.2, 0.2, 0.4, 0.8, 1.2, 1.6)


#### 1. GRanD & Change in Scarcity (Pessimistic 2050) ----
mapview(existing_dams, layer.name = 
          "Existing dams under<br/>pessimistic scenario for 2050<br/>–––––––––––––––––––––––––––<br/>Change in scarcity risk (color)<br/>Reservoir capacity (size)",
        zcol = "RC1_P50rc", cex = existing_dams$CapacityCex, label = existing_dams$Name,  
        col.regions = my_col.regions,
        at = legendBreaks,
        alpha.regions = 0.8,
        lwd = 0.1,
        viewer.suppress = FALSE,
        popup = leafpop::popupTable(existing_dams, zcol = c("Name", "Capacity", "Country", "Basin", "RC1", "RC1_P50", "RC1_P50rc"))
) %>% 
  leafem::addLogo( 
    #"https://panda.maps.arcgis.com/sharing/rest/content/items/eaf9bbe5243d4dca8073e3517517bd71/data", src = "remote", # Black
    "https://panda.maps.arcgis.com/sharing/rest/content/items/203af4ae947245948ad0958bc2c7ea9c/data", src = "remote", # White
    position = "bottomleft",
    offset.x = 20,
    offset.y = 330,
    width = 200,
    height = 40.5) %>%
  leaflet::setView(10, 20, 3)


#### 2. GRanD & Change in Flooding (Pessimistic 2050) ----
mapview(existing_dams, layer.name = 
          "Existing dams under<br/>pessimistic scenario for 2050<br/>–––––––––––––––––––––––––––<br/>Change in flood risk (color)<br/>Reservoir capacity (size)",
        zcol = "RC2_P50rc", cex = existing_dams$CapacityCex, label = existing_dams$Name,  
        col.regions = my_col.regions,
        at = legendBreaks,
        alpha.regions = 0.8,
        lwd = 0.1,
        viewer.suppress = FALSE,
        popup = leafpop::popupTable(existing_dams, zcol = c("Name", "Capacity", "Country", "Basin", "RC2", "RC2_P50", "RC2_P50rc"))
) %>% 
  leafem::addLogo( 
    #"https://panda.maps.arcgis.com/sharing/rest/content/items/eaf9bbe5243d4dca8073e3517517bd71/data", src = "remote", # Black
    "https://panda.maps.arcgis.com/sharing/rest/content/items/203af4ae947245948ad0958bc2c7ea9c/data", src = "remote", # White
    position = "bottomleft",
    offset.x = 20,
    offset.y = 330,
    width = 200,
    height = 40.5) %>%
  leaflet::setView(10, 20, 3)


#### 6a. GRanD & Biodiversity (2020) ----
mapview(existing_dams, layer.name = 
          "Existing dams risks to<br/>freshwater biodiversity<br/>–––––––––––––––––––––––––––<br/>Biodiversity risk in 2020 (color)<br/>Reservoir capacity (size)",
        zcol = "RC10", cex = existing_dams$CapacityCex, label = existing_dams$Name,  
        col.regions = colorRampPalette(c("#e9ffbe", "#ffd700", "#e60000")), 
        at = c(1, 1.4, 1.8, 2.2, 2.6, 3, 3.4, 3.8, 4.2, 4.6, 5),
        alpha.regions = 0.7,
        lwd = 0.1,
        viewer.suppress = FALSE,
        popup = leafpop::popupTable(existing_dams, zcol = c("Name", "Capacity", "Country", "Basin", "Basin", "RC10"))
) %>% 
  leafem::addLogo( 
    #"https://panda.maps.arcgis.com/sharing/rest/content/items/eaf9bbe5243d4dca8073e3517517bd71/data", src = "remote", # Black
    "https://panda.maps.arcgis.com/sharing/rest/content/items/203af4ae947245948ad0958bc2c7ea9c/data", src = "remote", # White
    position = "bottomleft",
    offset.x = 20,
    offset.y = 350,
    width = 200,
    height = 40.5) %>%
  leaflet::setView(10, 20, 3)





https://github.com/rafaexx/hydropowerClimateChange/blob/main/052_map_FHReD.R










