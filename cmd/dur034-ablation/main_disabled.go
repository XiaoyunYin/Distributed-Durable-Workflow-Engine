//go:build !dur034_ablation

package main

// DUR-034 is a measurement-only command. The default build keeps an inert
// command package; the actual ablation harness is compiled only with the
// explicit dur034_ablation tag by scripts/m7-dur034.ps1.
func main() {}
