# AfterDownloadCarer
Java program intended to cloud copy (via Google Drive Api) the recent downloaded items such as movies, episodes or seasons. Will interact with Radarr and Sonarr APIs

## Downloaded Element trigger
In the near future, this application will be triggered by torrent client when a downloaded has finished. Then, the application will copy the movie, the episode or the season to the proper location. And then notify Sonarr or Radarr of it, and also Plex so that the library will be refreshed too.
(Comming soon)

Meanwhile, the program will manage those "failed imports" on Sonarr and Radarr.

## Failed importing
Did you ever felt angry with those tons of failed imports? Success identifing the torrent, success downloading it, but then is not able to import (copy to Plex library location), like this image.
![image](https://user-images.githubusercontent.com/73612508/181925552-f8cb441c-6f38-48f8-941b-94dfe228ea9f.png)

So, when we realised something went wrong we enter to Sonarr, we identify those warning in the Queue, we click manually to import every failed element, repeating the process for ever season and episode downloaded... 

Fucking frustrating, to be honest.

### The solution
So, my application comes to the rescue identifying those failed imports, group by season and copy to the proper location. By the way, by ***Google Drive Cloud copy***, so the Sonarr server doesn't have to burn its CPU and bandwith downloading and uploading those GB via rclone mounted folders 🥰 
