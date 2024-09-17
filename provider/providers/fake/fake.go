package fake

import (
	"fmt"
	"math/rand"
)

type Fake struct {
	Mirrors []string
}

func (f Fake) Mirror() (string, error) {
	if len(f.Mirrors) == 0 {
		return "", fmt.Errorf("no mirrors available")
	}

	return f.Mirrors[rand.Intn(len(f.Mirrors))], nil
}
