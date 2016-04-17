package p9p

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

var chattyfuse bool

func PostService(fd int, name string, mtpt string) error {
	if name == "" && mtpt == "" {
		return errors.New("nothing to do")
	}
	if name != "" {
		var network, addr string
		if strings.Contains(name, "!") {
			// assume is already network address
			parts := strings.SplitN(name, "!", 2)
			network = parts[0]
			addr = parts[1]
		} else {
			ns, err := getns()
			if err != nil {
				return err
			}
			network = "unix"
			addr = fmt.Sprintf("%s/%s", ns, name)
		}

		nfd, err := syscall.Dup(fd)
		if err != nil {
			return err
		}
		cmd := exec.Command("9pserve", "-u", network+"!"+addr)
		cmd.Stdin = os.NewFile(uintptr(nfd), "in")
		cmd.Stdout = os.NewFile(uintptr(nfd), "out")
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "exec 9pserve: %v\n", err)
		}
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("9pserve failed: %v", err)
		}

		if mtpt != "" {
			/* reopen */
			if network == "unix" {
				fd, err = syscall.Open(addr, syscall.O_RDWR, 0)
				if err != nil {
					return fmt.Errorf("cannot reopen for mount: %v", err)
				}
			} else {
				panic("TODO")
				//if fd, err := dial(addr, nil, nil, nil); err != nil {
				//	return fmt.Errorf("cannot reopen for mount: %v", err)
				//}
			}
		}
	}
	if mtpt != "" {
		nfd, err := syscall.Dup(fd)
		if err != nil {
			return err
		}
		go func() {
			/* Try v9fs on Linux, which will mount 9P directly. */
			cmd := exec.Command("mount9p", "-", mtpt)
			cmd.Stdin = os.NewFile(uintptr(nfd), "in")
			cmd.Run()

			var err error
			if chattyfuse {
				err = exec.Command("9pfuse", "-D", "-", mtpt).Run()
			} else {
				err = exec.Command("9pfuse", "-", mtpt).Run()
			}
			fmt.Fprintf(os.Stderr, "exec 9pfuse: %v\n", err)
		}()
	}
	return nil
}
