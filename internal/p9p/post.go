package p9p

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"

	"9fans.net/go/plan9/client"
)

func PostService(conn net.Conn, name string) (string, error) {
	if name == "" {
		return "", errors.New("nothing to do")
	}

	ns := client.Namespace()
	addr := fmt.Sprintf("%s/%s", ns, name)

	cmd := exec.Command("9pserve", "-u", "unix!"+addr)
	cmd.Stdin = conn
	cmd.Stdout = conn
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("exec 9pserve: %v", err)
	}
	go func() {
		err := cmd.Wait()
		if err != nil {
			fmt.Fprintf(os.Stderr, "9pserve failed: %v", err)
		}
		conn.Close()
	}()

	return addr, nil
}
