package Hyades

import (
	"archive/zip"
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
)

func addtozip(z *zip.Writer, name string, f *os.File) {

	if fi, _ := f.Stat(); fi.IsDir() {
		log.Println("Adding folder", name)
		names, _ := f.Readdirnames(-1)
		for _, subname := range names {
			file, err := os.Open(filepath.Join(f.Name(), subname))
			if err != nil {
				log.Println(err)
				continue
			}
			addtozip(z, filepath.Join(name, subname), file)
			file.Close()
		}
	} else {
		log.Println("Adding file", name)
		fw, err := z.Create(name)
		if err != nil {
			panic(err)
		}
		_, err = io.Copy(fw, f)
		if err != nil {
			panic(err)
		}
	}
}

func ZipCompressFolder(folder string) []byte {
	envfile := new(bytes.Buffer)
	ZipCompressFolderWriter(folder, envfile)
	return envfile.Bytes()
}

func ZipCompressFolderWriter(folder string, writer io.Writer) {
	zipper := zip.NewWriter(writer)

	scan, err := os.Open(folder)
	if err != nil {
		panic(err)
	}
	defer scan.Close()

	log.Println("Calling addtozip")
	names, _ := scan.Readdirnames(-1)
	for _, subname := range names {
		file, err := os.Open(filepath.Join(folder, subname))
		if err != nil {
			continue
		}
		addtozip(zipper, subname, file)
		file.Close()
	}
	//addtozip(zipper,folder,scan)

	zipper.Close()
}
