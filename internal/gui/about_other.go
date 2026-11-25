//go:build !darwin && !nogui

package gui

import "github.com/wailsapp/wails/v3/pkg/application"

func setNativeAboutVersion(*application.Menu, string) {}
