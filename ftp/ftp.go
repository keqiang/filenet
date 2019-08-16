package ftp

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jlaffaye/ftp"
)

// ServerConfig defines the FTP server configuration
type ServerConfig struct {
	URL                string
	Port               int
	Username, Password string
}

// DownloadConfig is a config for files to download from a public FTP server
type DownloadConfig struct {
	ServerInfo     ServerConfig
	MaxConnection  int // max number of simultaneous connections
	BaseDir        string
	DestDir        string
	Files2Download []string
}

// NewDownloadConfig returns a basic DownloadConfig
func NewDownloadConfig(ftpURL, baseDir, destDir string, files2Download []string) *DownloadConfig {
	serverInfo := ServerConfig{
		URL:      ftpURL,
		Port:     21,
		Username: "anonymous",
		Password: "anonymous",
	}

	return &DownloadConfig{
		ServerInfo:     serverInfo,
		MaxConnection:  3,
		BaseDir:        baseDir,
		DestDir:        destDir,
		Files2Download: files2Download,
	}
}

// Download downloads files based on the config
func (fc DownloadConfig) Download() error {
	var wg sync.WaitGroup

	fileChannel := make(chan string)                         // init channel to add files
	wg.Add(1)                                                // adding files to channel
	go addFiles2Channel(fileChannel, fc.Files2Download, &wg) // must be async, otherwise the channel will block this statement

	wg.Add(fc.MaxConnection) // each worker will call wg.Done() when it's done

	if err := os.Mkdir(fc.DestDir, os.ModePerm); err == nil { // create the result directory
		for i := 0; i < fc.MaxConnection; i++ {
			go startDownloadWorker(fileChannel, fc, &wg)
		}
	} else {
		log.Fatal(err)
	}

	wg.Wait()
	return nil
}

func addFiles2Channel(fileChannel chan string, files []string, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(fileChannel)   // close the channel when done (so the range operator can operates on channel)
	for _, fn := range files { // add all files to a channel
		fileChannel <- fn
	}
}

func startDownloadWorker(fileChannel chan string, fc DownloadConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	for fileName := range fileChannel {
		handleDownload(fileName, fc)
	}
}

func handleDownload(fileName string, fc DownloadConfig) {
	baseFileName := filepath.Base(fileName) // strip path so this can be used as the name of output file
	log.Printf("Downloading file '%v'\n", baseFileName)

	ftpURL := fmt.Sprintf("%v:%v", fc.ServerInfo.URL, fc.ServerInfo.Port)
	c, err := ftp.Dial(ftpURL, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}

	defer c.Quit()

	err = c.Login(fc.ServerInfo.Username, fc.ServerInfo.Password)
	if err != nil {
		log.Fatal(err)
	}

	ftpDir := fc.BaseDir

	err = c.ChangeDir(ftpDir)
	if err != nil {
		log.Fatal(err)
	}

	res, err := c.Retr(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Close()

	outFile, err := os.Create(filepath.Join(fc.DestDir, baseFileName))
	defer outFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	_, err = io.Copy(outFile, res)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Downloaded file '%v'\n", baseFileName)
}
