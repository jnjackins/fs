package main

import (
	"bytes"
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

	ihash map[string]*User // lookup by .uid
	nhash map[string]*User // lookup by .uname
}

type User struct {
	uid    string
	uname  string
	leader string
	group  []string

	next *User
}

var ubox struct {
	lock sync.RWMutex
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

func _userByUid(box *Ubox, uid string) (*User, error) {
	if box != nil {
		if u, ok := box.ihash[uid]; ok {
			return u, nil
		}
	}
	return nil, fmt.Errorf("uname: uid %q not found", uid)
}

func unameByUid(uid string) string {
	ubox.lock.RLock()
	defer ubox.lock.RUnlock()

	if u, err := _userByUid(ubox.box, uid); err == nil {
		return u.uname
	}
	return ""
}

func _userByUname(box *Ubox, uname string) (*User, error) {
	if box != nil {
		if u, ok := box.nhash[uname]; ok {
			return u, nil
		}
	}

	return nil, fmt.Errorf("uname %q not found", uname)
}

func uidByUname(uname string) string {
	ubox.lock.RLock()
	defer ubox.lock.RUnlock()

	if u, err := _userByUname(ubox.box, uname); err == nil {
		return u.uid
	}
	return ""
}

func _groupMember(box *Ubox, group, member string, whenNoGroup bool) bool {
	/*
	 * Is 'member' a member of 'group'?
	 * Note that 'group' is a 'uid' and not a 'uname'.
	 * A 'member' is automatically in their own group.
	 */
	g, err := _userByUid(box, group)
	if err != nil {
		return whenNoGroup
	}
	m, err := _userByUname(box, member)
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
	u, err := _userByUname(box, member)
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

func groupMember(group, member string) bool {
	if group == "" {
		return false
	}

	ubox.lock.RLock()
	defer ubox.lock.RUnlock()

	return _groupMember(ubox.box, group, member, false)
}

func groupLeader(group, member string) bool {
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

func userAlloc(uid, uname string) *User {
	u := User{
		uid:   uid,
		uname: uname,
	}

	return &u
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

func writeUsersFile(box *Ubox) error {
	fsys, err := getFsys("main")
	if err != nil {
		return err
	}
	defer fsys.put()

	fsys.fsRlock()
	defer fsys.fsRUnlock()

	fs := fsys.getFs()

	/*
	 * BUG:
	 * 	the owner/group/permissions need to be thought out.
	 */

	dir, err := fs.openFile("/active")
	if err != nil {
		return err
	}
	file, err := dir.walk(uidadm)
	if err != nil {
		file, err = dir.create(uidadm, ModeDir|0775, uidadm)
	}
	dir.decRef()
	if err != nil {
		return err
	}
	dir = file
	file, err = dir.walk("users")
	if err != nil {
		file, err = dir.create("users", 0664, uidadm)
	}
	dir.decRef()
	if err != nil {
		return err
	}
	defer file.decRef()

	if err := file.truncate(uidadm); err != nil {
		return err
	}

	var buf bytes.Buffer
	for u := box.head; u != nil; u = u.next {
		buf.WriteString(fmt.Sprintf("%v\n", u))
	}

	if _, err := file.write(buf.Bytes(), box.length, 0, uidadm); err != nil {
		return err
	}

	return nil
}

func uboxRemUser(box *Ubox, u *User) {
	delete(box.ihash, u.uid)
	box.length -= len(u.uid)

	delete(box.nhash, u.uname)
	box.length -= len(u.uname)

	var up *User
	h := &box.head
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
	box.ihash[u.uid] = u
	box.length += len(u.uid)

	box.nhash[u.uname] = u
	box.length += len(u.uname)

	h := &box.head
	for up := *h; up != nil && up.uid < u.uid; up = up.next {
		h = &up.next
	}
	u.next = *h
	*h = u

	box.length += 4
	box.nuser++
}

func uboxDump(cons *Cons, box *Ubox) {
	cons.printf("nuser %d len = %d\n", box.nuser, box.length)

	for u := box.head; u != nil; u = u.next {
		cons.printf("%v\n", u)
	}
}

// TODO(jnj): don't log errors to stderr
func uboxInit(users string) error {
	/*
	 * Strip out whitespace and comments.
	 * Note that comments are pointless, they disappear
	 * when the server writes the database back out.
	 */
	var newline, comment bool
	var nline int

	buf := make([]rune, 0, len(users))
	blank := true
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
	box := &Ubox{
		ihash: make(map[string]*User),
		nhash: make(map[string]*User),
	}

	/*
	 * First pass - check format, check for duplicates
	 * and enter in hash buckets.
	 */
	lines := strings.Split(string(buf), "\n")
	validLines := make([]string, 0, len(lines))
	for _, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 4 {
			logf("bad line %q\n", line)
			continue
		}

		if fields[0] == "" || fields[1] == "" {
			logf("bad line %q\n", line)
			continue
		}

		if !validUserName(fields[0]) {
			logf("invalid uid %q\n", fields[0])
			continue
		}

		if _, err := _userByUid(box, fields[0]); err == nil {
			logf("duplicate uid %q\n", fields[0])
			continue
		}

		if !validUserName(fields[1]) {
			logf("invalid uname %q\n", fields[0])
			continue
		}

		if _, err := _userByUname(box, fields[1]); err == nil {
			logf("duplicate uname %q\n", fields[1])
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
					logf("failed to add %q to group %q: %v\n", member, g, err)
				}
			}
		}
	}

	for _, name := range usersMandatory {
		u, err := _userByUid(box, name)
		if err != nil {
			return fmt.Errorf("user %q is mandatory", name)
		}

		if u.uid != u.uname {
			return fmt.Errorf("uid/uname for user %q must match", name)
		}
	}

	ubox.lock.Lock()
	ubox.box = box
	ubox.lock.Unlock()

	return nil
}

func readUsersFile(path string) error {
	fsys, err := getFsys("main")
	if err != nil {
		return err
	}
	fsys.fsRlock()

	if path == "" {
		path = "/active/adm/users"
	}

	file, err := fsys.getFs().openFile(path)
	if err == nil {
		var size uint64
		if err = file.getSize(&size); err == nil {
			length := int(size)
			buf, err := file.read(int(size), 0)
			if len(buf) == length && err == nil {
				err = uboxInit(string(buf))
			}
		}
		file.decRef()
	}

	fsys.fsRUnlock()
	fsys.put()

	return err
}

func cmdUname(cons *Cons, argv []string) error {
	var usage string = "Usage: uname [-d] uname [uid|:uid|%%newname|=leader|+member|-member]"

	flags := flag.NewFlagSet("wstat", flag.ContinueOnError)
	flags.Usage = func() { fmt.Fprintln(os.Stderr, usage); flags.PrintDefaults() }
	dflag := flags.Bool("d", false, "Dump the contents of user table.")
	if err := flags.Parse(argv[1:]); err != nil {
		return EUsage
	}
	argv = flags.Args()
	argc := flags.NArg()

	if argc < 1 {
		if !*dflag {
			flags.Usage()
			return EUsage
		}
		ubox.lock.RLock()
		uboxDump(cons, ubox.box)
		ubox.lock.RUnlock()
		return nil
	}

	uname := argv[0]

	if argc == 1 {
		ubox.lock.RLock()
		u, err := _userByUname(ubox.box, uname)
		if err != nil {
			ubox.lock.RUnlock()
			return err
		}
		cons.printf("\t%v\n", u)
		ubox.lock.RUnlock()
		return nil
	}

	ubox.lock.Lock()
	defer ubox.lock.Unlock()

	u, err := _userByUname(ubox.box, uname)
	for _, arg := range argv[1:] {
		if arg[0] == '%' {
			if u == nil {
				return err
			}
			p := arg[1:]
			up, err := _userByUname(ubox.box, p)
			if err == nil {
				return fmt.Errorf("uname: uname %q already exists", up.uname)
			}
			for i := range usersMandatory {
				if usersMandatory[i] != uname {
					continue
				}
				return fmt.Errorf("uname: uname %q is mandatory", uname)
			}
			d := len(p) - len(u.uname)
			for up := ubox.box.head; up != nil; up = up.next {
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
		} else if arg[0] == '=' {
			if u == nil {
				return err
			}
			up, err := _userByUname(ubox.box, arg[1:])
			if err != nil {
				if len(arg) != 1 {
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
		} else if arg[0] == '+' {
			if u == nil {
				return err
			}
			up, err := _userByUname(ubox.box, arg[1:])
			if err != nil {
				return err
			}
			if err := _groupAddMember(ubox.box, u, up.uname); err != nil {
				return err
			}
		} else if arg[0] == '-' {
			if u == nil {
				return err
			}
			up, err := _userByUname(ubox.box, arg[1:])
			if err != nil {
				return err
			}
			if err := _groupRemMember(ubox.box, u, up.uname); err != nil {
				return err
			}
		} else {
			if u != nil {
				return fmt.Errorf("uname: uname %q already exists", u.uname)
			}
			uid := arg
			if uid[0] == ':' {
				uid = uid[1:]
			}
			u, err = _userByUid(ubox.box, uid)
			if err == nil {
				return fmt.Errorf("uname: uid %q already exists", uid)
			}
			u = userAlloc(uid, uname)
			uboxAddUser(ubox.box, u)
			if arg[0] != ':' {
				// should have an option for the mode and gid
				s := fmt.Sprintf("fsys main create /active/usr/%s %s %s d775", uname, uname, uname)
				if err := cliExec(cons, s); err != nil {
					return fmt.Errorf("create home dir: %v", err)
				}
			}
		}
	}
	if err := writeUsersFile(ubox.box); err != nil {
		return err
	}
	if *dflag {
		uboxDump(cons, ubox.box)
	}

	return nil
}

func cmdUsers(cons *Cons, argv []string) error {
	var usage string = "Usage: users [-d | -r file] [-w]"

	flags := flag.NewFlagSet("wstat", flag.ContinueOnError)
	flags.Usage = func() { fmt.Fprintln(os.Stderr, usage); flags.PrintDefaults() }
	var (
		dflag = flags.Bool("d", false, "Reset the user table with the default.")
		rflag = flags.String("r", "", "Read a user table from `file`, located in the \"main\" filesystem.")
		wflag = flags.Bool("w", false, "Write the tables to /active/adm/users on the \"main\" filesystem.")
	)
	if err := flags.Parse(argv[1:]); err != nil {
		return EUsage
	}
	if flags.NArg() != 0 {
		flags.Usage()
		return EUsage
	}

	file := *rflag

	if *dflag && file != "" {
		return fmt.Errorf("cannot use -d and -r together")
	}

	if *dflag {
		uboxInit(usersDefault)
	} else if file != "" {
		if err := readUsersFile(file); err != nil {
			return err
		}
	}

	ubox.lock.RLock()
	box := ubox.box
	cons.printf("\tnuser %d len %d\n", box.nuser, box.length)

	var err error
	if *wflag {
		err = writeUsersFile(box)
	}
	ubox.lock.RUnlock()
	return err
}

func usersInit() error {
	uboxInit(usersDefault)

	for _, err := range []error{
		cliAddCmd("users", cmdUsers),
		cliAddCmd("uname", cmdUname),
	} {
		if err != nil {
			return err
		}
	}

	return nil
}
