// Jupyter docs stats site logic

function updateDataAgeDisplay() {
    // Show live-updating data age (days since last datapoint)
    for (item of document.getElementsByClassName('latest_date_info')) {
        let dateItems = item.dataset.ageInfo.split(',');
        
        let elapsed = new Date() - new Date(dateItems[0], dateItems[1] - 1, dateItems[2]);
        item.innerText = 'Last Data: ' + (elapsed / (1000 * 60 * 60 * 24)).toFixed(1) + ' days ago';
    }
}

function run() {
    console.log('pooges4');
    let jupyterDocsData = {
        // TODO add any needed items here
    }
    window.jupyterDocsData = jupyterDocsData;

    updateDataAgeDisplay()
    setInterval(updateDataAgeDisplay, 1000);
}

document.addEventListener('DOMContentLoaded', run, false);
