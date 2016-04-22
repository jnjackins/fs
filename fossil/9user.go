package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
)

const (
	NUserHash = 1009
)

type Ubox struct {
	head   *User
	tail   *User
	nuser  int
	length int

	ihash [NUserHash]*User // lookup by .uid
	nhash [NUserHash]*User // lookup by .uname
}

type User struct {
	uid    string
	uname  string
	leader string
	group  []string

	next  *User
	ihash *User // lookup by .uid
	nhash *User // lookup by .uname
}

var ubox struct {
	lock *sync.RWMutex
	box  *Ubox
}

var usersDefault = `adm:adm:adm:sys
none:none::
noworld:noworld::
sys:sys::glenda
glenda:glenda:glenda:
`

var usersMandatory = []string{
	"adm",
	"none",
	"noworld",
	"sys",
}

var (
	uidadm     string = "adm"
	unamenone  string = "none"
	uidnoworld string = "noworld"
)

func userHash(s string) uint {
	var hash uint
	for i := 0; i < len(s); i++ {
		hash = hash*7 + uint(s[i])
	}
	return hash % NUserHash
}

func _userByUid(box *Ubox, uid string) (*User, error) {
	if box != nil {
		for u := box.ihash[userHash(uid)]; u != nil; u = u.ihash {
			if u.uid == uid {
				return u, nil
			}
		}
	}
	return nil, fmt.Errorf("uname: uid %q not found", uid)
}

func unameByUid(uid string) string {
	ubox.lock.RLock()
	u, err := _userByUid(ubox.box, uid)
	if err != nil {
		ubox.lock.RUnlock()
		return ""
	}

	uname := u.uname
	ubox.lock.RUnlock()

	return uname
}

func _userByUname(box *Ubox, uname string) (*User, error) {
	if box != nil {
		for u := box.nhash[userHash(uname)]; u != nil; u = u.nhash {
			if u.uname == uname {
				return u, nil
			}
		}
	}

	return nil, fmt.Errorf("uname %q not found", uname)
}

func uidByUname(uname string) string {
	ubox.lock.RLock()
	u, err := _userByUname(ubox.box, uname)
	if err != nil {
		ubox.lock.RUnlock()
		return ""
	}

	uid := u.uid
	ubox.lock.RUnlock()

	return uid
}

func _groupMember(box *Ubox, group string, member string, whenNoGroup bool) bool {
	var g *User
	var err error

	/*
	 * Is 'member' a member of 'group'?
	 * Note that 'group' is a 'uid' and not a 'uname'.
	 * A 'member' is automatically in their own group.
	 */
	g, err = _userByUid(box, group)
	if err != nil {
		return whenNoGroup
	}
	var m *User
	m, err = _userByUname(box, member)
	if m == nil {
		return false
	}
	if m == g {
		return true
	}
	for i := 0; i < len(g.group); i++ {
		if g.group[i] == member {
			return true
		}
	}

	return false
}

func groupWriteMember(uname string) bool {
	/*
	 * If there is a ``write'' group, then only its members can write
	 * to the file system, no matter what the permission bits say.
	 *
	 * To users not in the ``write'' group, the file system appears
	 * read only.  This is used to serve sources.cs.bell-labs.com
	 * to the world.
	 *
	 * Note that if there is no ``write'' group, then this routine
	 * makes it look like everyone is a member -- the opposite
	 * of what groupMember does.
	 *
	 * We use this for sources.cs.bell-labs.com.
	 * If this slows things down too much on systems that don't
	 * use this functionality, we could cache the write group lookup.
	 */
	ubox.lock.RLock()
	defer ubox.lock.RUnlock()

	return _groupMember(ubox.box, "write", uname, true)
}

func _groupRemMember(box *Ubox, g *User, member string) error {
	if _, err := _userByUname(box, member); err != nil {
		return err
	}

	var i int
	for i = 0; i < len(g.group); i++ {
		if g.group[i] == member {
			break
		}
	}

	if i >= len(g.group) {
		var err error
		if g.uname == member {
			err = fmt.Errorf("uname: %q always in own group", member)
		} else {
			err = fmt.Errorf("uname: %q not in group %q", member, g.uname)
		}
		return err
	}

	box.length -= len(member)
	if len(g.group) > 1 {
		box.length--
	}

	newlength := len(g.group) - 1
	switch newlength {
	case 0:
		g.group = nil
	default:
		for ; i < newlength; i++ {
			g.group[i] = g.group[i+1]
		}
		g.group = g.group[:newlength]
	}

	return nil
}

func _groupAddMember(box *Ubox, g *User, member string) error {
	var u *User
	var err error

	u, err = _userByUname(box, member)
	if err != nil {
		return err
	}
	if _groupMember(box, g.uid, u.uname, false) {
		var err error
		if g.uname == member {
			err = fmt.Errorf("uname: %q always in own group", member)
		} else {
			err = fmt.Errorf("uname: %q already in group %q", member, g.uname)
		}
		return err
	}

	g.group = append(g.group, member)
	box.length += len(member)
	if len(g.group) > 1 {
		box.length++
	}

	return nil
}

func groupMember(group string, member string) bool {
	if group == "" {
		return false
	}

	ubox.lock.RLock()
	defer ubox.lock.RUnlock()

	return _groupMember(ubox.box, group, member, false)
}

func groupLeader(group string, member string) bool {
	/*
	 * Is 'member' the leader of 'group'?
	 * Note that 'group' is a 'uid' and not a 'uname'.
	 * Uname 'none' cannot be a group leader.
	 */
	if member == unamenone || group == "" {
		return false
	}

	ubox.lock.RLock()
	defer ubox.lock.RUnlock()

	var g *User
	g, err := _userByUid(ubox.box, group)
	if err != nil {
		return false
	}

	if g.leader != "" {
		if g.leader == member {
			return true
		}
		return false
	} else {
		return _groupMember(ubox.box, group, member, false)
	}
}

func userAlloc(uid string, uname string) *User {
	u := new(User)
	u.uid = uid
	u.uname = uname

	return u
}

func validUserName(name string) bool {
	return !strings.ContainsAny(name, "#:,()")
}

func (u *User) String() string {
	s := fmt.Sprintf("%s:%s:", u.uid, u.uname)
	if u.leader != "" {
		s += u.leader
	}
	s += ":"
	if len(u.group) > 0 {
		s += u.group[0]
		for _, g := range u.group[1:] {
			s += "," + g
		}
	}
	return s
}

func usersFileWrite(box *Ubox) error {
	var err error

	fsys, err := fsysGet("main")
	if err != nil {
		return err
	}
	defer fsysPut(fsys)

	fsysFsRlock(fsys)
	defer fsysFsRUnlock(fsys)

	fs := fsysGetFs(fsys)

	/*
	 * BUG:
	 * 	the owner/group/permissions need to be thought out.
	 */

	var dir *File
	dir, err = fileOpen(fs, "/active")
	if err != nil {
		return err
	}
	var file *File
	file, err = fileWalk(dir, uidadm)
	if err != nil {
		file, err = fileCreate(dir, uidadm, ModeDir|0775, uidadm)
	}
	fileDecRef(dir)
	var buf bytes.Buffer
	if err != nil {
		goto tidy
	}
	dir = file
	file, err = fileWalk(dir, "users")
	if err != nil {
		file, err = fileCreate(dir, "users", 0664, uidadm)
	}
	fileDecRef(dir)
	if err != nil {
		goto tidy
	}
	if err = fileTruncate(file, uidadm); err != nil {
		goto tidy
	}

	for u := box.head; u != nil; u = u.next {
		buf.WriteString(fmt.Sprintf("%v\n", u))
	}

	_, err = fileWrite(file, buf.Bytes(), box.length, 0, uidadm)

tidy:
	if file != nil {
		fileDecRef(file)
	}

	return err
}

func uboxRemUser(box *Ubox, u *User) {
	var up *User

	h := &box.ihash[userHash(u.uid)]
	for up = *h; up != nil && up != u; up = up.ihash {
		h = &up.ihash
	}
	assert(up == u)
	*h = up.ihash
	box.length -= len(u.uid)

	h = &box.nhash[userHash(u.uname)]
	for up = *h; up != nil && up != u; up = up.nhash {
		h = &up.nhash
	}
	assert(up == u)
	*h = up.nhash
	box.length -= len(u.uname)

	h = &box.head
	for up = *h; up != nil && up.uid != u.uid; up = up.next {
		h = &up.next
	}
	assert(up == u)
	*h = u.next
	u.next = nil

	box.length -= 4
	box.nuser--
}

func uboxAddUser(box *Ubox, u *User) {
	h := &box.ihash[userHash(u.uid)]
	u.ihash = *h
	*h = u
	box.length += len(u.uid)

	h = &box.nhash[userHash(u.uname)]
	u.nhash = *h
	*h = u
	box.length += len(u.uname)

	h = &box.head
	for up := *h; up != nil && up.uid < u.uid; up = up.next {
		h = &up.next
	}
	u.next = *h
	*h = u

	box.length += 4
	box.nuser++
}

func uboxDump(box *Ubox) {
	consPrintf("nuser %d len = %d\n", box.nuser, box.length)

	for u := box.head; u != nil; u = u.next {
		consPrintf("%v\n", u)
	}
}

func uboxInit(users string) error {
	/*
	 * Strip out whitespace and comments.
	 * Note that comments are pointless, they disappear
	 * when the server writes the database back out.
	 */
	var newline, comment, blank bool
	var nline int

	buf := make([]rune, 0, len(users))
	blank = true
	for _, c := range users {
		if c == '\r' || c == '\t' || c == ' ' {
			continue
		}
		if c == '\n' {
			if !blank {
				if !newline {
					newline = true
					buf = append(buf, '\n')
					nline++
				}
				blank = true
			}
			comment = false
			continue
		}
		if c == '#' {
			comment = true
		}
		blank = false
		if !comment {
			newline = false
			buf = append(buf, c)
		}
	}
	if buf[len(buf)-1] == '\n' {
		buf = buf[:len(buf)-1]
	}

	/*
	 * Everything is updated in a local Ubox until verified.
	 */
	box := new(Ubox)

	/*
	 * First pass - check format, check for duplicates
	 * and enter in hash buckets.
	 */
	lines := strings.Split(string(buf), "\n")
	validLines := make([]string, 0, len(lines))
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 4 {
			fmt.Fprintf(os.Stderr, "bad line %q\n", line)
			continue
		}

		if fields[0] == "" || fields[1] == "" {
			fmt.Fprintf(os.Stderr, "bad line %q\n", line)
			continue
		}

		if !validUserName(fields[0]) {
			fmt.Fprintf(os.Stderr, "invalid uid %q\n", fields[0])
			continue
		}

		if _, err := _userByUid(box, fields[0]); err == nil {
			fmt.Fprintf(os.Stderr, "duplicate uid %q\n", fields[0])
			continue
		}

		if !validUserName(fields[1]) {
			fmt.Fprintf(os.Stderr, "invalid uname %q\n", fields[0])
			continue
		}

		if _, err := _userByUname(box, fields[1]); err == nil {
			fmt.Fprintf(os.Stderr, "duplicate uname %q\n", fields[1])
			continue
		}

		u := userAlloc(fields[0], fields[1])
		uboxAddUser(box, u)
		validLines = append(validLines, line)
	}

	/*
	 * Second pass - fill in leader and group information.
	 */
	for _, line := range validLines {
		fields := strings.Split(line, ":")
		g, err := _userByUname(box, fields[1])
		if err != nil {
			panic("impossible")
		}
		if fields[2] != "" {
			u, err := _userByUname(box, fields[2])
			if err != nil {
				g.leader = g.uname
			} else {
				g.leader = u.uname
			}
			box.length += len(g.leader)
		}

		if fields[3] != "" {
			for _, member := range strings.Split(fields[3], ",") {
				if err := _groupAddMember(box, g, member); err != nil {
					fmt.Fprintf(os.Stderr, "failed to add %q to group %q: %v\n", member, g, err)
				}
			}
		}
	}

	for _, name := range usersMandatory {
		u, err := _userByUid(box, name)
		if err != nil {
			err = fmt.Errorf("user %q is mandatory", name)
			return err
		}

		if u.uid != u.uname {
			err = fmt.Errorf("uid/uname for user %q must match", name)
			return err
		}
	}

	ubox.lock.Lock()
	ubox.box = box
	ubox.lock.Unlock()

	return nil
}

func usersFileRead(path_ string) error {
	fsys, err := fsysGet("main")
	if err != nil {
		return err
	}
	fsysFsRlock(fsys)

	if path_ == "" {
		path_ = "/active/adm/users"
	}

	var file *File
	var buf []byte
	file, err = fileOpen(fsysGetFs(fsys), path_)
	if err == nil {
		var size uint64
		if err = fileGetSize(file, &size); err == nil {
			length := int(size)
			buf, err = fileRead(file, int(size), 0)
			if len(buf) == length && err == nil {
				err = uboxInit(string(buf))
			}
		}
		fileDecRef(file)
	}

	fsysFsRUnlock(fsys)
	fsysPut(fsys)

	return err
}

func cmdUname(argv []string) error {
	var createfmt string = "fsys main create /active/usr/%s %s %s d775"
	var usage string = "usage: uname [-d] uname [uid|:uid|%%newname|=leader|+member|-member]"

	flags := flag.NewFlagSet("wstat", flag.ContinueOnError)
	dflag := flags.Bool("d", false, "")
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()

	if argc < 1 {
		if !*dflag {
			return errors.New(usage)
		}
		ubox.lock.RLock()
		uboxDump(ubox.box)
		ubox.lock.RUnlock()
		return nil
	}

	uname := argv[0]
	argc--
	argv = argv[1:]

	if argc == 0 {
		ubox.lock.RLock()
		var err error
		var u *User
		u, err = _userByUname(ubox.box, uname)
		if u == nil {
			ubox.lock.RUnlock()
			return err
		}

		consPrintf("\t%v\n", u)
		ubox.lock.RUnlock()
		return nil
	}

	ubox.lock.Lock()
	defer ubox.lock.Unlock()

	u, err := _userByUname(ubox.box, uname)
	var up *User
	for {
		tmp1 := argc
		argc--
		if tmp1 == 0 {
			break
		}
		if argv[0][0] == '%' {
			if u == nil {
				return err
			}

			p := argv[0][1:]
			up, err = _userByUname(ubox.box, p)
			if err == nil {
				return fmt.Errorf("uname: uname %q already exists", up.uname)
			}

			for i := 0; usersMandatory[i] != ""; i++ {
				if usersMandatory[i] != uname {
					continue
				}
				return fmt.Errorf("uname: uname %q is mandatory", uname)
			}

			d := len(p) - len(u.uname)
			for up = ubox.box.head; up != nil; up = up.next {
				if up.leader != "" {
					if up.leader == u.uname {
						up.leader = p
						ubox.box.length += d
					}
				}

				for i := 0; i < len(up.group); i++ {
					if up.group[i] != u.uname {
						continue
					}
					up.group[i] = p
					ubox.box.length += d
					break
				}
			}

			uboxRemUser(ubox.box, u)
			u.uname = p
			uboxAddUser(ubox.box, u)
		} else if argv[0][0] == '=' {
			if u == nil {
				return err
			}

			up, err = _userByUname(ubox.box, argv[0][1:])
			if err != nil {
				if len(argv[0]) != 1 {
					return err
				}
			}

			if u.leader != "" {
				ubox.box.length -= len(u.leader)
				u.leader = ""
			}

			if up != nil {
				u.leader = up.uname
				ubox.box.length += len(u.leader)
			}
		} else if argv[0][0] == '+' {
			if u == nil {
				return err
			}

			up, err = _userByUname(ubox.box, argv[0][1:])
			if up == nil {
				return err
			}

			if err = _groupAddMember(ubox.box, u, up.uname); err != nil {
				return err
			}
		} else if argv[0][0] == '-' {
			if u == nil {
				return err
			}

			up, err = _userByUname(ubox.box, argv[0][1:])
			if err != nil {
				return err
			}

			if err = _groupRemMember(ubox.box, u, up.uname); err != nil {
				return err
			}
		} else {
			if u != nil {
				return fmt.Errorf("uname: uname %q already exists", u.uname)
			}

			uid := argv[0]
			if uid[0] == ':' {
				uid = uid[1:]
			}
			u, err = _userByUid(ubox.box, uid)
			if err == nil {
				return fmt.Errorf("uname: uid %q already exists", uid)
			}

			u = userAlloc(uid, uname)
			uboxAddUser(ubox.box, u)
			if argv[0][0] != ':' {
				// should have an option for the mode and gid
				s := fmt.Sprintf(createfmt, uname, uname, uname)
				err = cliExec(s)
				if err != nil {
					return err
				}
			}
		}

		argv = argv[1:]
	}

	if err := usersFileWrite(ubox.box); err != nil {
		return err
	}

	if !*dflag {
		uboxDump(ubox.box)
	}

	return nil
}

func cmdUsers(argv []string) error {
	var usage string = "usage: users [-d | -r file] [-w]"

	flags := flag.NewFlagSet("wstat", flag.ContinueOnError)
	dflag := flags.Bool("d", false, "")
	rflag := flags.String("r", "", "file")
	wflag := flags.Bool("w", false, "")
	if err := flags.Parse(argv[1:]); err != nil {
		return fmt.Errorf(usage)
	}
	argv = flags.Args()
	argc := flags.NArg()

	file := *rflag

	if argc != 0 {
		return fmt.Errorf(usage)
	}

	if *dflag && file != "" {
		return fmt.Errorf("cannot use -d and -r together")
	}

	if *dflag {
		uboxInit(usersDefault)
	} else if file != "" {
		if err := usersFileRead(file); err != nil {
			return err
		}
	}

	ubox.lock.RLock()
	box := ubox.box
	consPrintf("\tnuser %d len %d\n", box.nuser, box.length)

	var err error
	if *wflag {
		err = usersFileWrite(box)
	}
	ubox.lock.RUnlock()
	return err
}

func usersInit() error {
	ubox.lock = new(sync.RWMutex)
	uboxInit(usersDefault)

	cliAddCmd("users", cmdUsers)
	cliAddCmd("uname", cmdUname)

	return nil
}
