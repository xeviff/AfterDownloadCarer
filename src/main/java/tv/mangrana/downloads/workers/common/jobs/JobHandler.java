package tv.mangrana.downloads.workers.common.jobs;

import org.apache.commons.lang.StringUtils;
import tv.mangrana.config.ConfigFileLoader;
import tv.mangrana.downloads.workers.common.ElementHandler;
import tv.mangrana.downloads.workers.common.JobOrchestrator;
import tv.mangrana.downloads.workers.radarr.jobs.RadarrJobHandler;
import tv.mangrana.downloads.workers.sonarr.jobs.SonarrJobFile;
import tv.mangrana.downloads.workers.sonarr.jobs.SonarrJobHandler;
import tv.mangrana.exception.IncorrectWorkingReferencesException;
import tv.mangrana.exception.NoElementFoundException;
import tv.mangrana.exception.TooMuchTriesException;
import tv.mangrana.google.api.client.RemoteCopyService;
import tv.mangrana.jobs.JobFile;
import tv.mangrana.jobs.JobFileManager;
import tv.mangrana.utils.EasyLogger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.Duration;
import java.time.Instant;

public abstract class JobHandler implements Runnable {

    protected EasyLogger logger;

    protected ConfigFileLoader configFileLoader;
    protected RemoteCopyService copyService;

    @SuppressWarnings("rawtypes")
    protected JobFile jobFile;
    final JobOrchestrator orchestrator;

    protected String jobTitle="-not_set-";
    protected String fullTitle;
    protected String elementName;
    protected String fileName;
    protected String downloadId;

    @SuppressWarnings("rawtypes")
    protected JobHandler(ConfigFileLoader configFileLoader, JobFile jobFile, JobOrchestrator caller) throws IOException, IncorrectWorkingReferencesException {
        this.configFileLoader = configFileLoader;
        copyService = new RemoteCopyService(configFileLoader);
        this.jobFile = jobFile;
        orchestrator = caller;
        try {
            loadInfoFromJobFile();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IncorrectWorkingReferencesException("A problem was risen when getting info from file: "+e.getMessage());
        }
    }
    protected abstract void loadInfoFromJobFile();
    public void tryToMoveIfPossible() throws IOException, IncorrectWorkingReferencesException, NoElementFoundException, TooMuchTriesException {
        if (StringUtils.isEmpty(fileName)) {
            logger.nLog("this job has not a fileName retrieved, so it cannot be processed.");
        } else {
            elementName = fileName;
            logger.nLog("going to try handle the following element: "+downloadId);
            getElementHandler().crashHandle();
            jobFile.forceMarkDone();
        }
    }

    @Override
    public void run() {
        boolean error=false;
        try {
            logger.nLog("going to handle the so called <{0}>", fullTitle);
            setJobStateInitiated();
            if (StringUtils.isNotEmpty(fileName)) {
                logger.nLog("Retrieved successfully from file the cached element name: <{0}> :D", fileName);
                elementName = fileName;
            } else {
                retrieveFileNameFromArrApp();
                writeElementNameToJobInfo(elementName);
            }
            setJobStateWorkingOrSleep();
            getElementHandler().handle();
            jobFile.markDone();
        } catch (Exception e) {
            error=true;
            logger.nLog("something wrong: {0}", e.getMessage());
            jobFile.driveBack();
            e.printStackTrace();
        } finally {
            setJobStateFinished(error);
        }
    }
    protected abstract ElementHandler getElementHandler() throws IOException;

    protected abstract void retrieveFileNameFromArrApp() throws TooMuchTriesException;

    private void writeElementNameToJobInfo(String elementName) throws IOException {
        try (Writer output = new BufferedWriter(
                new FileWriter(jobFile.getFile().getAbsolutePath(), true))) {
            output.append(SonarrJobFile.GrabInfo.JAVA_FILENAME.name().toLowerCase().concat(": "+elementName));
            logger.nLog("persisted elementName to job file -> "+elementName);
        }
    }

    private void setJobStateWorkingOrSleep() {
        synchronized (orchestrator) {
            orchestrator.jobHasFileName(this);
            if (orchestrator.isWorkingWithAJob()) {
                try {
                    Instant beforeWait = Instant.now();
                    while (orchestrator.isWorkingWithAJob()) {
                        orchestrator.wait();
                    }
                    Instant afterWait = Instant.now();
                    if (Duration.between(beforeWait, afterWait).toMinutes()<1) {
                        logger.nHLog("seems that things are going too fast between job sleep and its resume");
                    }
                } catch (InterruptedException e) {
                    logger.nHLog("could not put on waiting the job {0}", jobTitle);
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }
            jobFile.markDoing();
            orchestrator.jobWorking(this);
        }
    }

    private void setJobStateInitiated() {
        synchronized (orchestrator) {
            orchestrator.jobInitiated(this);
        }
    }

    private void setJobStateFinished(boolean error) {
        synchronized (orchestrator) {
            if (error) {
                orchestrator.jobError(this);
            } else {
                orchestrator.jobFinished(this);
            }
            orchestrator.notifyAll();
        }
    }

    protected void logWhenActive(String msg, Object... params){
        if (!orchestrator.isWorkingWithAJob()
                || orchestrator.isJobWorking(this)) {
            logger.nLog(msg, params);
        }
    }

    public String getFullTitle() {
        return fullTitle;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    @SuppressWarnings("rawtypes")
    public JobFile getJobFile() {
        return jobFile;
    }

    public JobFileManager.JobFileType getJobType() {
        if (this instanceof RadarrJobHandler)
            return JobFileManager.JobFileType.RADARR_JOBS;
        else if (this instanceof SonarrJobHandler)
            return JobFileManager.JobFileType.SONARR_JOBS;
        else return null;
    }

}
