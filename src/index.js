
function processDates() {
    for (item of document.getElementsByClassName('latest_date_info')) {
        
        let raw = item.innerText.split(',');
        console.log("WIK WIK")
        console.log(item.innerText)
        let elapsed = new Date() - new Date(raw[0], raw[1] - 1, raw[2]);
        item.innerText = 'Days since last datapoint: ~' + (elapsed / (1000 * 60 * 60 * 24));
        item.style.visibility = "visible";
    }
}

document.addEventListener('DOMContentLoaded', processDates, false);
