package winfsp

import "strings"

// splitPath breaks a WinFsp path into the components to walk. WinFsp's FUSE
// layer turns every Windows separator into "/" before the adapter sees the
// path, and turns a backslash that is part of a name (shown to Windows as
// U+F05C) back into a backslash. So "/" is the only separator: a backslash is a
// character of the name, as on every other mount. Empty or "." components have
// to drop out rather than become a lookup for a name that does not exist.
func splitPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	parts := make([]string, 0, strings.Count(trimmed, "/")+1)
	for _, name := range strings.Split(trimmed, "/") {
		if name == "" || name == "." {
			continue
		}
		parts = append(parts, name)
	}
	return parts
}

// splitParent separates the components leading up to the final name, which the
// create and delete operations need apart. ok is false for the root, which has
// no parent to operate in.
func splitParent(path string) (parent []string, name string, ok bool) {
	parts := splitPath(path)
	if len(parts) == 0 {
		return nil, "", false
	}
	return parts[:len(parts)-1], parts[len(parts)-1], true
}
