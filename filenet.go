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

// CheckBinaryExistence checks if the underlying OS has the binary in this user's PATH
func CheckBinaryExistence(binaryFileName string) error {
	_, err := exec.LookPath(binaryFileName) // check if the specified binary is installed in the system
	if err != nil {
		return fmt.Errorf("Can not locate binary file '%v' on your system; check if it's installed and is added to your PATH variable", binaryFileName)
	}
	return nil
}
