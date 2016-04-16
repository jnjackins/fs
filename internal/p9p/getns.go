package p9p

import (
	"fmt"
	"os"
)

/*
func isme(uid string) bool {
	char * p

	n, err := strconv.ParseInt(uid, 10, 0)
	if err == nil {
		return n == getuid()
	}
	return getuser() == uid
}
*/

/*
 * Absent other hints, it works reasonably well to use
 * the X11 display name as the name space identifier.
 * This is how sam's B has worked since the early days.
 * Since most programs using name spaces are also using X,
 * this still seems reasonable.  Terminal-only sessions
 * can set $NAMESPACE.
 */
/*
func nsfromdisplay() (string, error) {
	disp := os.Getenv("DISPLAY")
	if disp == "" {
		if runtime.GOOS == "darwin" {
			// Might be running native GUI on OS X.
			disp = ":0.0"
		} else {
			return "", errors.New("$DISPLAY not set");;
		}
	}

	// canonicalize: xxx:0.0 => xxx:0
	i = strings.IndexReverse(disp, ':');
	if i >= 0 {
		i++
		for isdigit(disp[i]) {
			i++;
		}
		if disp[i:] == ".0" {
			disp = disp[:i]
		}
	}

	// turn /tmp/launch/:0 into _tmp_launch_:0 (OS X 10.5)
	for p=disp; *p; p++ {
		if(*p == '/') {
			*p = '_';
		}
	}

	p = smprint("/tmp/ns.%s.%s", getuser(), disp);
	free(disp);
	if(p == nil){
		werrstr("out of memory");
		return p;
	}
	if((fd=create(p, OREAD, DMDIR|0700)) >= 0){
		close(fd);
		return p;
	}
	if((d = dirstat(p)) == nil){
		free(d);
		werrstr("stat %s: %r", p);
		free(p);
		return nil;
	}
	if((d->mode&0777) != 0700 || !isme(d->uid)){
		werrstr("bad name space dir %s", p);
		free(p);
		free(d);
		return nil;
	}
	free(d);
	return p;
}
*/

func getns() (string, error) {
	var err error

	ns := os.Getenv("NAMESPACE")
	if ns == "" {
		err = fmt.Errorf("$NAMESPACE not set")
	}

	return ns, err
}
