# Generated terminal walkthrough; render with: pwsh -File scripts/render-recruiter-demo.ps1
Require bash
Output demos/recruiter-demo.gif
Set Shell "bash"
Set Width 1100
Set Height 640
Set FontSize 15
Set Padding 18
Set TypingSpeed 35ms

Type "echo 'A real workflow through the deployed local stack'"
Enter
Type "./.scratch/dur045/local-demo-linux -api $VHS_API_URL"
Enter
Wait+Screen /invariant checker: PASS/
Sleep 2s
