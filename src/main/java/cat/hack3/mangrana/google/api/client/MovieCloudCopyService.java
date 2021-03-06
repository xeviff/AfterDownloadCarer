package cat.hack3.mangrana.google.api.client;

import cat.hack3.mangrana.config.ConfigFileLoader;
import cat.hack3.mangrana.google.api.client.gateway.GoogleDriveApiGateway;
import com.google.api.services.drive.model.File;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static cat.hack3.mangrana.google.api.client.gateway.GoogleDriveApiGateway.GoogleElementType.FOLDER;
import static cat.hack3.mangrana.google.api.client.gateway.GoogleDriveApiGateway.GoogleElementType.VIDEO;
import static cat.hack3.mangrana.utils.Output.log;

public class MovieCloudCopyService {

    ConfigFileLoader configFileLoader;
    GoogleDriveApiGateway googleDriveApiGateway;

    public MovieCloudCopyService(ConfigFileLoader configFileLoader) throws IOException {
        this.configFileLoader = configFileLoader;
        googleDriveApiGateway = new GoogleDriveApiGateway();
    }

    public void copyVideoFile(String downloadedFileName, String destinationFullPath) throws IOException {
        File videoFile = googleDriveApiGateway
                .lookupElementByName(downloadedFileName, VIDEO, configFileLoader.getDownloadsTDid());
        if (Objects.nonNull(videoFile)) {
            String destinationFolderId = resolveFolderIdByPath(destinationFullPath);
            googleDriveApiGateway
                    .copyFile(videoFile.getId(), destinationFolderId);
            log(">> copied successfully!! :D ");
            log("fileName: "+downloadedFileName);
            log("fileId: "+videoFile.getId());
            log("destinationFolderName: "+destinationFullPath);
            log("destinationFolderId: "+destinationFolderId);
        } else {
            log("Video element not found in google drive :( " + downloadedFileName);
        }
    }

    private String resolveFolderIdByPath(String destinationFullPath) throws IOException {
        String destinationFolderName = destinationFullPath.substring(destinationFullPath.lastIndexOf("/")+1);
        try {
            return searchFolderByName(destinationFolderName);
        } catch (NoSuchElementException e) {
            log("failed finding the folder but we'll try to create it");
            try {
                return createFolder(getParentDirectoryFromAbsolute(destinationFullPath), destinationFolderName)
                        .getId();
            } catch (IOException e2) {
                log("couldn't create the folder as well, so I surrender");
                throw e2;
            }
        }
    }

    private String searchFolderByName(String destinationFolderName) throws IOException {
        return googleDriveApiGateway
                .lookupElementByName(destinationFolderName, FOLDER, configFileLoader.getMoviesTDid())
                .getId();
    }

    private File createFolder(String parentDirectory, String destinationFolderName) throws IOException {
        String parentFolderId = searchFolderByName(parentDirectory);
        return googleDriveApiGateway.createFolder(destinationFolderName, parentFolderId);
    }

    private String getParentDirectoryFromAbsolute(String absolutePath){
        return Paths
                .get(absolutePath)
                .getParent()
                .getFileName()
                .toString();
    }

}
