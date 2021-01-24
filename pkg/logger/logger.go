package logger

import (
	"log"
	"os"
)

var (
	debugLogger   *log.Logger
	infoLogger    *log.Logger
	warningLogger *log.Logger
	errorLogger   *log.Logger
	fatalLogger   *log.Logger
)

func Debug(format string, v ...interface{}) {
	debugLogger.Printf(format, v...)
}

func Info(format string, v ...interface{}) {
	infoLogger.Printf(format, v...)
}

func Warning(format string, v ...interface{}) {
	warningLogger.Printf(format, v...)
}

func Error(format string, v ...interface{}) {
	errorLogger.Printf(format, v...)
}

func Fatal(format string, v ...interface{}) {
	fatalLogger.Fatalf(format, v...)
}

func init() {
	flags := log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC | log.Lmsgprefix
	debugLogger = log.New(os.Stderr, "D | ", flags)
	infoLogger = log.New(os.Stderr, "I | ", flags)
	warningLogger = log.New(os.Stderr, "W | ", flags)
	errorLogger = log.New(os.Stderr, "E | ", flags)
	fatalLogger = log.New(os.Stderr, "F | ", flags)
}
