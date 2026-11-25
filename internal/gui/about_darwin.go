//go:build !nogui

package gui

/*
#cgo CFLAGS: -x objective-c
#cgo LDFLAGS: -framework Cocoa
#import <Cocoa/Cocoa.h>
#include <stdlib.h>

static void showAboutVersion(const char *version) {
	[NSApp orderFrontStandardAboutPanelWithOptions:@{
		NSAboutPanelOptionApplicationVersion: [NSString stringWithUTF8String:version]
	}];
}
*/
import "C"

import (
	"unsafe"

	"github.com/wailsapp/wails/v3/pkg/application"
)

func setNativeAboutVersion(menu *application.Menu, version string) {
	if about := menu.FindByRole(application.About); about != nil {
		about.SetRole(application.NoRole).OnClick(func(*application.Context) {
			application.InvokeSync(func() {
				value := C.CString(version)
				defer C.free(unsafe.Pointer(value))
				C.showAboutVersion(value)
			})
		})
	}
}
