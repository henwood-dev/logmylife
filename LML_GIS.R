library(geosphere)
library(bit64)
library(sp)
library(rgdal)
library(rgeos)
library(raster)
library(ggmap)
library(leaflet)
library(htmltools)
library(htmlwidgets)
library(aspace)

# Override
extract <- tidyr::extract
shift <- data.table::shift
summarize <- dplyr::summarize
slice <- dplyr::slice

# Imports
#source("LML_Tidy.R", encoding = "UTF-8")
#source("LML_Baseline.R", encoding = "UTF-8")

gps_geocode_address <- function(){
  site_addresses <- fread("/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Geospatial/site_geocoding.csv")
  api_key <- names(fread("/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Geospatial/api_key.txt"))
  register_google(api_key)
  for(i in 1:nrow(site_addresses))
  {
    result <- geocode(site_addresses$location[i], output = "latlona", source = "google")
    site_addresses$longitude[i] <- as.numeric(result[1])
    site_addresses$latitude[i] <- as.numeric(result[2])
    site_addresses$verifyaddress[i] <- as.character(result[3])
  }
  housing_geo <- select(site_addresses,housing_building_name,longitude,latitude)
  write_csv(housing_geo,"/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Geospatial/geocoded_housing.csv")
  return(housing_geo)
}

mapitout <- function(gps_data){
  leaflet() %>%
    addTiles() %>%
    addCircles(data = gps_data)
}

heatitup <- function(gps_data, radius = 10){
  registerPlugin <- function(map, plugin) {
    map$dependencies <- c(map$dependencies, list(plugin))
    map
  }
  heatPlugin <- htmlDependency("Leaflet.heat", "99.99.99",
                               src = c(href = "http://leaflet.github.io/Leaflet.heat/dist/"),
                               script = "leaflet-heat.js"
  )
  
  leaflet() %>%
    addTiles() %>%
    fitBounds(min(gps_data$longitude), min(gps_data$latitude), max(gps_data$longitude), max(gps_data$latitude)) %>%
    registerPlugin(heatPlugin) %>%
    onRender(paste0("function(el, x, data) {
                    data = HTMLWidgets.dataframeToD3(data);
                    data = data.map(function(val) { return [val.latitude, val.longitude]; });
                    L.heatLayer(data, {radius: ",radius,"}).addTo(this);
}"), data = gps_data %>% select(latitude, longitude))
}

buffer_zone <- function(housing_geo, get_housing_name, data_id_only, domap = FALSE, fast = FALSE) {
  house_point <- housing_geo %>%
    filter(housing_building_name == get_housing_name) %>%
    select(longitude,latitude)
  if(nrow(house_point)==0){
    data_id_only[, athome := NA]
    return(data_id_only)
  } else {
    house_sp <- SpatialPoints(coords = house_point, proj4string = CRS("+init=epsg:4326"))
    house_buffer <- buffer(house_sp, width=150)
    
    gps_data <- as.data.table(data_id_only)
    if(!fast){
      for(i in 1:nrow(gps_data)){
        point_loc <- SpatialPoints(coords = gps_data[i,c("longitude","latitude")], proj4string = CRS("+init=epsg:4326"))
        doesint <- gIntersects(house_buffer,point_loc)
        gps_data[i, athome := doesint]
      }
    } else {
      point_loc <- SpatialPoints(coords = gps_data[,c("longitude","latitude")], proj4string = CRS("+init=epsg:4326"))
      over_array <- !is.na(over(point_loc,house_buffer))
      gps_data[, athome := over_array]
    }
    if(!domap){
      return(gps_data)
    } else{
      point_loc <- SpatialPoints(coords = gps_data[,c("longitude","latitude")], proj4string = CRS("+init=epsg:4326"))
      leaflet(house_buffer) %>%
        addPolygons() %>%
        addTiles() %>%
        addMarkers(data = point_loc)
    }
  }
}

enhance_gps <- function(gps, new_enroll, housing_geo, day_level = FALSE){
  convert_gps <- gps %>%
    mutate(pid = as.numeric(file_id)) %>%
    mutate(date = as.Date(fulltime, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
    as.data.table()
  
  core_count <- detectCores() - 1L
  registerDoParallel(cores = core_count) 
  
  enhanced_gps <- foreach(i = unique(convert_gps$pid), .combine = "rbind") %dopar% {
    #print(i)
    #i = 1026
    enroll_housing <- as.character(filter(new_enroll,pid == i)[1,"housing_building_name"])
    id_only_data <- convert_gps[pid == i]
    buffer_zone(housing_geo,enroll_housing,id_only_data, fast = TRUE)
  }
  
  day_space <- gps_per_day(gps)
  merge_day <- enhanced_gps %>%
    left_join(day_space, by = c("file_id","date")) %>%
    as.data.table()
  
  if(day_level){
    merge_day <- merge_day %>%
      group_by(pid,date) %>%
      summarize(day_lat = mean(latitude, na.rm = TRUE), day_long = mean(longitude, na.rm = TRUE), day_pts = n(),
                athome_prop = mean(athome, na.rm = TRUE), mch_area = mean(mch_area_km, na.rm = TRUE), sde_area = mean(sde_area_km, na.rm = TRUE)) %>%
      as.data.table()
  }
  
  return(merge_day)
}


gps_per_day <- function(gps_data = NULL, multicore = TRUE) {
  if(is.null(gps_data)){
    data_dirname <- "/Users/eldin/University of Southern California/LogMyLife Project - Documents/Data/Prompt Level"
    gps <- read_gps(data_dirname, wockets_dirname, manual_dirname, TRUE, TRUE) %>%
      mutate(date = as.Date(fulltime, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
      as.data.table() 
  } else {
    gps <- gps_data %>%
      mutate(date = as.Date(fulltime, origin = "1970-01-01", tz = "America/Los_Angeles")) %>%
      as.data.table()
  }
  gps_id_day <- gps %>%
    group_by(file_id,date) %>%
    summarize(day_lat = mean(latitude), day_long = mean(longitude), day_pts = n()) %>%
    as.data.table()
  
  gps_ref_temp <- function(gps, gps_id_day, ipos) {
    temp_df <- gps[date == gps_id_day$date[ipos] & file_id == gps_id_day$file_id[ipos]]
    temp_df <- temp_df[, c("longitude","latitude"), with = FALSE]
    attr(temp_df, "old_row") <- rownames(gps_id_day[ipos])[1]
    return(temp_df)
  }
  
  if(!multicore){
    temp_df <- gps_ref_temp(gps,gps_id_day,1)
    con.hull.pos <- chull(temp_df) # find positions of convex hull
    
    if(!is_empty(con.hull.pos)){
      con.hull <- rbind(temp_df[con.hull.pos,],temp_df[con.hull.pos[1],]) # get coordinates for convex hull
      coordinates(con.hull) <- c("longitude","latitude")
      proj4string(con.hull) <- CRS("+init=epsg:4326")
      p <- Polygon(con.hull)
      ps <- Polygons(list(p),attr(temp_df,"old_row"))
      sps <- SpatialPolygons(list(ps))
      proj4string(sps) <- CRS("+init=epsg:4326")
      sps_proj <- spTransform(sps,CRS("+init=epsg:26945"))
      area_km <- gArea(sps_proj)/(1000*1000)
    } else{
      area_km <- NA
    }
    gps_id_day[1,mch_area_km := area_km]
    
    for(i in 2:nrow(gps_id_day)){
      print(i)
      temp_df <- gps_ref_temp(gps,gps_id_day,i)
      
      con.hull.pos <- chull(temp_df) # find positions of convex hull
      if(!is_empty(con.hull.pos)){
        con.hull <- rbind(temp_df[con.hull.pos,],temp_df[con.hull.pos[1],]) # get coordinates for convex hull
        coordinates(con.hull) <- c("longitude","latitude")
        proj4string(con.hull) <- CRS("+init=epsg:4326")
        p <- Polygon(con.hull)
        ps <- Polygons(list(p),attr(temp_df,"old_row"))
        pre_sps <- SpatialPolygons(list(ps))
        proj4string(pre_sps) <- CRS("+init=epsg:4326")
        sps <- bind(sps,pre_sps)
        sps_proj <- spTransform(pre_sps,CRS("+init=epsg:26945"))
        area_km <- gArea(sps_proj)/(1000*1000)
      } else{
        area_km <- NA
      }
      gps_id_day[i,mch_area_km := area_km]
    }
    projected <- spTransform(sps,CRS("+init=epsg:26945"))
    spatial_gps_id_day <- SpatialPolygonsDataFrame(sps,gps_id_day, match.ID = TRUE)
    
    for(j in unique(gps_id_day$file_id)){
      print(j)
      subset_sdf <- subset(spatial_gps_id_day, file_id == j)  
      subset_sp <- SpatialPolygons(subset_sdf@polygons, proj4string = subset_sdf@proj4string)
      subset_st <- spTransform(subset_sp,CRS("+init=epsg:26945"))
      
      base_intersect <- gIntersection(subset_st[1],subset_st[2])
      for(i in 2:(length(subset_st)-1)){
        base_intersect <- gIntersection(base_intersect,subset_st[i+1])
      }
      gps_id_day[file_id == j, overlap := gArea(base_intersect)]
    }
  } else {
    core_count <- detectCores() - 1L
    registerDoParallel(cores = core_count) 
    
    area_km <- foreach(i = 1:nrow(gps_id_day), .combine = "c") %dopar% {
      temp_df <- gps_ref_temp(gps,gps_id_day,i)
      con.hull.pos <- chull(temp_df) # find positions of convex hull
      if(!is_empty(con.hull.pos)){
        con.hull <- rbind(temp_df[con.hull.pos,],temp_df[con.hull.pos[1],]) # get coordinates for convex hull
        coordinates(con.hull) <- c("longitude","latitude")
        proj4string(con.hull) <- CRS("+init=epsg:4326")
        p <- Polygon(con.hull)
        ps <- Polygons(list(p),attr(temp_df,"old_row"))
        pre_sps <- SpatialPolygons(list(ps))
        proj4string(pre_sps) <- CRS("+init=epsg:4326")
        #sps <- bind(sps,pre_sps)
        sps_proj <- spTransform(pre_sps,CRS("+init=epsg:26945"))
        gArea(sps_proj)/(1000*1000)
      } else {
        NA
      }
    }
    gps_id_day[, mch_area_km := area_km]
    
    area_km <- foreach(i = 1:nrow(gps_id_day), .combine = "c") %dopar% {
      temp_df <- gps_ref_temp(gps,gps_id_day,i)
      sde_points <- tryCatch(
        {
          calc_sde(points = temp_df[, list(longitude, latitude)])
        },
        error=function(cond) {
          NULL
        },
        warning=function(cond) {
          NULL
        },
        finally={}
      )    
      if(!is_empty(sde_points)){
        sde_points <- na.omit(sde_points)
        if(nrow(sde_points)>0){
          sde_poly <- Polygon(sde_points[, c("x","y")])
          sde_mp <- Polygons(list(sde_poly),i)
          sde_sp <- SpatialPolygons(list(sde_mp))
          proj4string(sde_sp) <- CRS("+init=epsg:4326")
          sps_proj <- spTransform(sde_sp,CRS("+init=epsg:26945"))
          gArea(sps_proj)/(1000*1000)
        } else {
          NA
        }
      } else {
        NA
      }
    }
    gps_id_day[, sde_area_km := area_km]
  }
  
  return(gps_id_day)
}