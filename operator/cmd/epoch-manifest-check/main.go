// Command epoch-manifest-check strictly validates rendered Epoch Kubernetes resources.
package main

import (
	"fmt"
	"os"

	"epoch.local/epoch/operator/internal/manifests"
)

func main() {
	count, err := manifests.Validate(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "epoch-manifest-check:", err)
		os.Exit(1)
	}
	fmt.Printf("validated %d Kubernetes objects\n", count)
}
