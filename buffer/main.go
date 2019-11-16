package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
)

func init() {
	session, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2")},
	)

	uploader = s3manager.NewUploader(session)
	downloader = s3manager.NewDownloader(session)
}

func main() {
	files := []*s3.GetObjectInput{
		&s3.GetObjectInput{
			Key:    aws.String("file1.txt"),
			Bucket: aws.String("to-be-zipped/"),
		},
		&s3.GetObjectInput{
			Key:    aws.String("file2.txt"),
			Bucket: aws.String("to-be-zipped/"),
		},
	}

	upload := &s3manager.UploadInput{
		Key:    aws.String("zipped-files"),
		Bucket: aws.String("zipped-destination"),
	}

	zipFromS3AndUploadToS3(files, upload)
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func zipFromS3AndUploadToS3(in []*s3.GetObjectInput, result *s3manager.UploadInput) {
	filesToZip := make([]string, 0, len(in))

	for _, file := range in {
		pathToFile := os.TempDir() + "/" + path.Base(*file.Key)
		f, _ := os.Create(pathToFile)
		downloader.Download(f, file)
		f.Close()
		filesToZip = append(filesToZip, pathToFile)
	}

	zipFile := os.TempDir() + "/" + path.Base(*result.Key)
	f, _ := os.Create(zipFile)
	defer f.Close()
	zipWriter := zip.NewWriter(f)
	for _, file := range filesToZip {
		w, _ := zipWriter.Create(file)
		inFile, _ := os.Open(file)
		io.Copy(w, inFile)
		inFile.Close()
	}
	zipWriter.Close()
	f.Seek(0, 0)

	result.Body = f
	_, err := uploader.Upload(result)

	if err != nil {
		exitErrorf("Unable to upload %v", err)
	}
}
