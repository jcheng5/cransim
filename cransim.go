package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"time"
)

var chanNextLine chan string = make(chan string)
var conns []chan string = make([]chan string, 0, 0)
var chanRegister chan chan string = make(chan chan string)
var chanUnregister chan chan string = make(chan chan string)
const timeOffset = 28 * 24 * time.Hour
const addr = ":6789"

func sync() error {
	today := virtualNow().Truncate(24 * time.Hour)

	// Delete obsolete files
	files, err := ioutil.ReadDir("data")
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		t, err := time.Parse("2006-01-02.csv.gz", file.Name())
		if err != nil {
			continue
		}
		if t.Before(today) {
			log.Println("Deleting", "data/" + file.Name())
			err = os.Remove("data/" + file.Name())
			if err != nil {
				log.Println("Error:", err)
			}
		}
	}

	for i := 0; i < 2; i++ {
		year := today.AddDate(0, 0, i).Format("2006")
		fname := today.AddDate(0, 0, i).Format("2006-01-02") + ".csv.gz"
		fi, err := os.Stat("data/" + fname)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err != nil || fi.Size() < 30 {
			log.Println("Downloading " + fname)
			cmd := exec.Command("bash", "-c", "curl http://cran-logs.rstudio.com/"+year+"/"+fname+" | zcat | sort | gzip > data/"+fname)
			err = cmd.Run()
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func handleConn(conn net.Conn) {
	log.Println("Connection opened from", conn.RemoteAddr())
	ch := make(chan string, 1000)
	chanRegister <- ch
	go doWrites(conn, ch)

	buf := make([]byte, 1, 1)
	conn.Read(buf)
	chanUnregister <- ch
	conn.Close()
	close(ch)
	log.Println("Connection closed from ", conn.RemoteAddr())
}

func doWrites(conn net.Conn, ch chan string) {
	for {
		str, ok := <-ch
		if !ok {
			return
		}
		conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		_, err := conn.Write([]byte(str))
		if err != nil {
			conn.Close()
			return
		}
	}
}

func virtualNow() time.Time {
	return time.Now().UTC().Add(-timeOffset)
}

type Scanner struct {
	dir      string
	reader   io.ReadCloser
	gzreader *gzip.Reader
	scanner  *bufio.Scanner
	nextDate time.Time
}

func NewScanner(dir string) *Scanner {
	return &Scanner{dir: dir, nextDate: virtualNow()}
}

func (s *Scanner) NextLine() (string, error) {
	if s.scanner == nil {
		filename := s.dir + "/" + s.nextDate.Format("2006-01-02") + ".csv.gz"
		fi, err := os.Stat(filename)
		if err != nil {
			return "", err
		}
		if fi.Size() < 30 {
			// Failed downloads are 20 bytes for some reason
			return "", errors.New("Ignoring empty file")
		}
		r, err := os.Open(filename)
		if err != nil {
			return "", err
		}
		log.Println("Scanning file", filename)
		s.nextDate = s.nextDate.Add(24 * time.Hour)
		s.reader = r
		s.gzreader, err = gzip.NewReader(s.reader)
		if err != nil {
			return "", err
		}
		s.scanner = bufio.NewScanner(s.gzreader)
	}

	if s.scanner.Scan() {
		return s.scanner.Text(), nil
	}
	scanErr := s.scanner.Err()
	s.reader.Close()
	s.gzreader.Close()
	s.reader = nil
	s.gzreader = nil
	s.scanner = nil

	if scanErr != nil {
		return "", scanErr
	} else {
		return s.NextLine()
	}
}

func data() {
	firstLine := false
	scanner := NewScanner("data")
	for {
		nextLine, err := scanner.NextLine()
		if err != nil {
			log.Println("Error:", err)
			time.Sleep(1 * time.Second)
			continue
		}
		timeStr := nextLine[:len("\"2014-11-14\",\"06:07:18\"")]
		val, err := time.Parse("\"2006-01-02\",\"15:04:05\"", timeStr)
		if err != nil {
			log.Println("Error parsing time:", err)
			continue
		}

		wait := val.Sub(virtualNow())
		if wait < 0 && firstLine {
			log.Println("Skipping", )
			continue
		}
		if wait > 0 {
			time.Sleep(wait)
		}
		chanNextLine <- nextLine + "\n"
	}
}

func service() {
	for {
		select {
		case ch := <-chanRegister:
			conns = append(conns, ch)
			log.Println(len(conns), "active connection(s)")
		case ch := <-chanUnregister:
			found := false
			for i, el := range conns {
				if el == ch {
					found = true
					conns[i] = nil
					conns = append(conns[:i], conns[i+1:]...)
					break
				}
			}
			if !found {
				log.Println("Couldn't find channel to unregister!")
			} else {
				log.Println(len(conns), "active connection(s)")
			}
		case str := <-chanNextLine:
			for _, ch := range conns {
				select {
				case ch <- str: break
				default: break
				}
			}
		}
	}
}

func main() {
	err := os.MkdirAll("data", 0755)
	if err != nil {
		log.Fatal(err)
	}

	err = sync()
	if err != nil {
		log.Fatal(err)
	}

	go (func() {
		for {
			time.Sleep(30 * time.Second)
			sync()
		}
	})()

	go service()
	go data()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		go handleConn(conn)
	}
}
