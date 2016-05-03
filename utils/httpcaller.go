package utils

import _ "fmt"
import "net/http"
import "net/url"
import "strings"


func MakeCall(apiUrl string, content string) {
	hc := http.Client{}
    form := url.Values{}
    form.Add("log", content)
    // fmt.Printf("Sending: %s\n", content)
    req, err := http.NewRequest("POST", apiUrl, strings.NewReader(form.Encode()))
    if err != nil {
    	
	}
    req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
    resp, err := hc.Do(req)
    
    if err != nil {
		
	}else{
		if resp.Status == "201 Created"{
			//fmt.Println("Success\n\n")
		} else{
			//fmt.Printf("%s\n", resp.Status)
		}
		defer resp.Body.Close()
	}
}