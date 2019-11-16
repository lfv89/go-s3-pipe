package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

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

type FakeWriterAt struct {
	w io.Writer
}

func (fw FakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	return fw.w.Write(p)
}

func zipFromS3AndUploadToS3(in []*s3.GetObjectInput, result *s3manager.UploadInput) {
	pr, pw := io.Pipe()
	zipWriter := zip.NewWriter(pw)
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer func() {
			wg.Done()
			zipWriter.Close()
			pw.Close()
		}()
		for _, file := range in {
			w, err := zipWriter.Create(path.Base(*file.Key))
			if err != nil {
				fmt.Println(err)
			}
			_, err = downloader.Download(FakeWriterAt{w}, file)
			if err != nil {
				fmt.Println(err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		result.Body = pr
		_, err := uploader.Upload(result)
		if err != nil {
			fmt.Println(err)
		}
	}()

	wg.Wait()
}
