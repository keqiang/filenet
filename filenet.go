package filenet

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/jlaffaye/ftp"
)

// DownloadFileAtURL will download a url to a local file
func DownloadFileAtURL(url, outputFilePath string) error {
	// Get data at the url
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Write the body to file
	return writeToFile(outputFilePath, resp.Body)
}

// DecompressFiles decompress files specified in a map. Each entry in the map is a mapping from file to decompress -> name after decompression
func DecompressFiles(files2Decompress map[string]string, maxWorkerNumber int) {
	// TODO implement actual worker logic
	if maxWorkerNumber > 5 {
		maxWorkerNumber = 5
	}
	var wg sync.WaitGroup
	wg.Add(len(files2Decompress))
	for gzFile, unzippedFile := range files2Decompress {
		go func(src, dst string) {
			defer wg.Done()
			err := GZipDecompress(src, dst)
			if err != nil {
				log.Fatal(err)
			}
		}(gzFile, unzippedFile)
	}
	wg.Wait()
}

// GZipDecompress decompress a zipped file
func GZipDecompress(compressedFilePath, outputFilePath string) error {
	log.Printf("Decompressing file '%v'\n", filepath.Base(compressedFilePath))
	fi, err := os.Open(compressedFilePath) // open file as a file handler
	if err != nil {
		return err
	}
	defer fi.Close()
	fz, err := gzip.NewReader(fi)
	if err != nil {
		return err
	}
	defer fz.Close()
	err = writeToFile(outputFilePath, fz)
	if err != nil {
		return err
	}
	log.Printf("Decompressed to file '%v'\n", filepath.Base(outputFilePath))
	return nil
}

// reader from a Reader object and write to a file
func writeToFile(filePath string, src io.Reader) error {
	outFile, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, src)
	return err
}

// FTPDownloadConfig is a config for files to download from a public FTP server
type FTPDownloadConfig struct {
	URL            string
	Port           int
	MaxConnection  int // max number of simultaneous connections
	BaseDir        string
	DestDir        string
	Files2Download []string
}

// Download downloads files based on the config
func (fc FTPDownloadConfig) Download() error {
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

func startDownloadWorker(fileChannel chan string, fc FTPDownloadConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	for fileName := range fileChannel {
		handleDownload(fileName, fc)
	}
}

func handleDownload(fileName string, fc FTPDownloadConfig) {
	baseFileName := filepath.Base(fileName) // strip path so this can be used as the name of output file
	log.Printf("Downloading file '%v'\n", baseFileName)

	ftpURL := fmt.Sprintf("%v:%v", fc.URL, fc.Port)
	c, err := ftp.Dial(ftpURL, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}

	defer c.Quit()

	err = c.Login("anonymous", "anonymous")
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

// CheckBinaryExistence checks if the underlying OS has the binary in this user's PATH
func CheckBinaryExistence(binaryFileName string) error {
	_, err := exec.LookPath(binaryFileName) // check if the specified binary is installed in the system
	if err != nil {
		return fmt.Errorf("Can not locate binary file '%v' on your system; check if it's installed and is added to your PATH variable", binaryFileName)
	}
	return nil
}
