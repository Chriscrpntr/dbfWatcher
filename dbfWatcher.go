package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/LindsayBradford/go-dbf/godbf"
	_ "github.com/alexbrainman/odbc"
	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Array for easy dbfImport function usage
type imports struct {
	filename string
	csvName  string
	mp       map[int]string
}

var dbfImports = []imports{
	{"Example.DBF",
		"Example_CSV_Name",
		map[int]string{0: "int", 1: "int", 71: "int"}},
}

func logError(err error) {
	// Check for errors and log them
	if err != nil {
		file, openErr := os.OpenFile("logs.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if openErr != nil {
			fmt.Println(openErr)
			return
		}
		// Write error to logs.txt file
		_, writeErr := io.WriteString(file, err.Error()+"\n")
		if writeErr != nil {
			fmt.Println(writeErr)
		}
		// Close logs.txt
		closeErr := file.Close()
		if closeErr != nil {
			fmt.Println(closeErr)
		}
	}
}

func sqlUpdate() {

	// Open the SQL file
	sqlFile, err := os.Open(os.Getenv("SQL_QUERY_FILE"))
	if err != nil {
		logError(err)
		return
	}
	// Close the SQL file
	defer func(sqlFile *os.File) {
		err := sqlFile.Close()
		if err != nil {
			logError(err)
			return
		}
	}(sqlFile)

	// Read the SQL file
	sqlQuery, err := io.ReadAll(sqlFile)
	if err != nil {
		logError(err)
		return
	}
	// Database login variables
	pass := os.Getenv("DB_PASSWORD")
	user := os.Getenv("DB_USERNAME")
	dbAddress := os.Getenv("DB_ADDRESS")
	dbPort := os.Getenv("DB_PORT")
	dbname := os.Getenv("DB_NAME")
	// Create the DB connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", dbAddress, dbPort, user, pass, dbname)

	// Open the database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logError(err)
		return
	}
	// Close the database connection
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logError(err)
			return
		}
	}(db)

	// Execute the SQL query
	_, err = db.Exec(string(sqlQuery))
	if err != nil {
		logError(err)
		return
	}
}

func dbfImport(dir string, dbfFile string, filename string, indices map[int]string) error {
	// Read FoxPro dbf file
	df, err := godbf.NewFromFile(dir+dbfFile, "UTF-8")
	if err != nil {
		logError(err)
	}
	// Create a new file for writing CSV data
	f, err := os.Create(os.Getenv("CSV_DIR") + filename + ".csv")
	if err != nil {
		logError(err)
	}
	// Close CSV file
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			logError(err)
		}
	}(f)

	// Create a CSV writer
	writer := csv.NewWriter(f)
	defer writer.Flush()
	// Print filename in terminal
	fmt.Println(filename)
	// Write header row with field names
	header := make([]string, len(df.FieldNames()))

	for i, field := range df.FieldNames() {
		if _, ok := indices[i]; ok {
			header[i] = field
		} else {
			header[i] = ""
		}
	}

	if err := writer.Write(header); err != nil {
		logError(err)
	}

	// Write data rows
	for i := 0; i < df.NumberOfRecords(); i++ {
		record := df.GetRowAsSlice(i)
		data := make([]string, 0, len(record))
		for i, field := range record {
			if fieldType, ok := indices[i]; ok {
				// Type conversions
				switch fieldType {
				case "float":
					if f, err := strconv.ParseFloat(field, 64); err == nil {
						data = append(data, strconv.FormatFloat(f, 'g', -1, 64))
					} else {
						data = append(data, "") // Empty string for parsing errors
					}
				case "int":
					if n, err := strconv.Atoi(field); err == nil {
						data = append(data, strconv.Itoa(n))
					} else {
						data = append(data, "") // Empty string for parsing errors
					}
				case "date":
					if t, err := time.Parse("20060102", field); err == nil {
						data = append(data, t.Format("2006-01-02"))
					} else if t, err := time.Parse("01/02/06", field); err == nil {
						data = append(data, t.Format("2006-01-02"))
					} else if t, err := time.Parse("01022006", field); err == nil {
						data = append(data, t.Format("2006-01-02"))
					} else {
						data = append(data, "")
					}
				default:
					data = append(data, field)
				}
			}
		}

		if err := writer.Write(data); err != nil {
			return err
		}
	}

	return nil
}

func watch(dir string) {

	restart := make(chan bool)
	for {
		// Watcher to detect changes in the directory
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			logError(err)
		}
		defer func(watcher *fsnotify.Watcher) {
			err := watcher.Close()
			if err != nil {
				logError(err)
			}
		}(watcher)

		err = watcher.Add(dir)
		if err != nil {
			logError(err)
			_ = watcher.Close()
			return
		}
		go func() {
			fmt.Println("Watcher Starting...")
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered from panic in watcher Goroutine:", r)
					fmt.Println("Watcher Restarting...")
					restart <- true
				} else {
					restart <- false
				}
				_ = watcher.Close()
			}()
			for {
				select {
				// Heartbeat to check the watcher is still running
				case <-time.After(5 * time.Minute):
					fmt.Println("Heartbeat: watcher is still running", time.Now().Format("15:04:05"))
				case event, ok := <-watcher.Events:
					if !ok {
						logError(err)
						fmt.Println("Watcher Restarting...")
						restart <- true
					}
					fmt.Println(event)
					// If an event is detected, run the dbfImport function
					if event.Has(fsnotify.Write) {
						for _, i := range dbfImports {
							dbfFilename := filepath.Base(i.filename)
							csvFilename := strings.TrimSuffix(dbfFilename, ".DBF")
							if filepath.Base(event.Name) == dbfFilename {
								log.Println("modified file:", dbfFilename)

								fmt.Printf("%s Updating...\n", dbfFilename)
								err := dbfImport(dir, dbfFilename, csvFilename, i.mp)
								if err != nil {
									logError(err)
								}
								// After the conversion to CSV is done run the sql to import it to Postgresql
								sqlUpdate()
								fmt.Printf("%s Finished!\n", dbfFilename)
								break
							}
						}
					}
				case err, ok := <-watcher.Errors:
					if !ok {
						logError(err)
						log.Println("error:", err)
						return
					}
				}
			}

		}()
		if <-restart {
			continue
		} else {
			break
		}
	}
}

func main() {

	// Panic handler
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", r)
		}
	}()

	// Load env variables
	err := godotenv.Load(".env")
	if err != nil {
		logError(err)
		log.Fatal("Error loading .env file")
	}

	workingDir := os.Getenv("FOXPRO_DIR")
	go watch(workingDir)

	// In case Watcher misses a change or crashes, time based update to ensure all data is up-to-date
	go func() {
		for {
			fmt.Println("Time-based update at ", time.Now().Format("15:04"))
			for _, i := range dbfImports {
				err := dbfImport(workingDir, i.filename, i.csvName, i.mp)
				if err != nil {
					logError(err)
				}
			}
			// After the conversion to CSV is done run the sql to import it to Postgresql
			sqlUpdate()
			fmt.Println("Time-based update finished! at", time.Now().Format("15:04"))
			time.Sleep(300 * time.Second)
		}
	}()
	select {}
}
