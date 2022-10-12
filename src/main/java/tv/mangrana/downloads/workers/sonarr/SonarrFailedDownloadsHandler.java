package tv.mangrana.downloads.workers.sonarr;

import tv.mangrana.config.ConfigFileLoader;
import tv.mangrana.downloads.workers.common.Handler;
import tv.mangrana.downloads.workers.sonarr.bean.Season;
import tv.mangrana.exception.IncorrectWorkingReferencesException;
import tv.mangrana.exception.NoElementFoundException;
import tv.mangrana.exception.TooMuchTriesException;
import tv.mangrana.google.api.client.RemoteCopyService;
import tv.mangrana.plex.url.PlexCommandLauncher;
import tv.mangrana.sonarr.api.client.gateway.SonarrApiGateway;
import tv.mangrana.sonarr.api.schema.queue.Record;
import tv.mangrana.sonarr.api.schema.queue.SonarrQueue;
import tv.mangrana.sonarr.api.schema.series.SonarrSerie;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static tv.mangrana.utils.Output.log;
import static tv.mangrana.utils.StringCaptor.getSeasonFolderNameFromEpisode;
import static tv.mangrana.utils.StringCaptor.getSeasonFolderNameFromSeason;
import static java.util.stream.Collectors.groupingBy;
@Deprecated
public class SonarrFailedDownloadsHandler implements Handler {

    SonarrApiGateway sonarrApiGateway;
    RemoteCopyService copyService;
    PlexCommandLauncher plexCommander;
    SerieRefresher serieRefresher;


    public SonarrFailedDownloadsHandler(ConfigFileLoader configFileLoader) throws IOException {
        sonarrApiGateway = new SonarrApiGateway(configFileLoader);
        copyService = new RemoteCopyService(configFileLoader);
        plexCommander = new PlexCommandLauncher(configFileLoader);
        serieRefresher = new SerieRefresher(configFileLoader);
    }

    public void handle () {
        log("this is the sonarr failed downloads handler. Scanning queue...");
        SonarrQueue queue = sonarrApiGateway.getQueue();
        processRecords(queue.getRecords());
    }

    private void processRecords(List<Record> queue) {
        Map<String, List<Record>> recordsGroupedByTitle =
            queue
                .stream()
                .collect(groupingBy(Record::getTitle));

        handleSeasons(
                recordsGroupedByTitle.entrySet()
                    .stream()
                    .filter(recordGroup -> recordGroup.getValue().size() > 1)
                    .map(this::buildSeason)
                    .collect(Collectors.toList()));

        handleSingleEpisodes(
                recordsGroupedByTitle.values()
                        .stream()
                        .filter(records -> records.size() == 1)
                        .map(list -> list.get(0))
                        .collect(Collectors.toList()));
    }

    private void handleSeasons(List<Season> recordsByTitle) {
        log("going to handle seasons");
        recordsByTitle.forEach(this::handleSeason);
    }

    private void handleSingleEpisodes(List<Record> episodeRecords) {
        log("going to handle episodes");
        episodeRecords.forEach(this::handleEpisode);
    }

    private void handleSeason(Season season) {
        try {
            SonarrSerie serie = sonarrApiGateway.getSerieById(season.getSerieId());
            copyService.copySeasonFromDownloadToItsLocation(
                    season.getDownloadedFolderName(),
                    serie.getPath(),
                    getSeasonFolderNameFromSeason(season.getDownloadedFolderName())
            );
            serieRefresher.refreshSerieInSonarrAndPlex(serie, season.getQueueItemId());
            log("season handled!");
        } catch (IncorrectWorkingReferencesException | IOException | NoElementFoundException e) {
            log("could not handle the season because of "+e.getMessage());
            e.printStackTrace();
        } catch (TooMuchTriesException e) {
            log("The season {0} treatment will be skipped for the moment due to so many retries", season.getDownloadedFolderName());
        }
    }

    private void handleEpisode(Record episodeRecord) {
        try {
            String absolutePathWithFile = episodeRecord.getOutputPath();
            SonarrSerie serie = sonarrApiGateway.getSerieById(episodeRecord.getSeriesId());
            copyService.copyEpisodeFromDownloadToItsLocation(
                    absolutePathWithFile.substring(absolutePathWithFile.lastIndexOf('/')+1),
                    serie.getPath(),
                    getSeasonFolderNameFromEpisode(episodeRecord.getTitle())
            );
            serieRefresher.refreshSerieInSonarrAndPlex(serie, episodeRecord.getId());
            log("episode handled!");
        } catch (Exception e) {
            log("could not handle the episode because of "+e.getMessage());
            e.printStackTrace();
        }
    }

    private Season buildSeason(Map.Entry<String, List<Record>> entry) {
        return new Season(entry.getKey(), entry.getValue().get(0));
    }

}