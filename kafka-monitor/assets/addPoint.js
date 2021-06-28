function addPoint(addPloiNClicks,colocData,predData,polFigure,name,meta,lastClick,fixedPoints)
{
    let trigger = dash_clientside.callback_context.triggered;
    const updatedFixedPoints = fixedPoints;
    if (trigger.length === 0) {
        return [polFigure, updatedFixedPoints];
    }

    const data = polFigure.data;
    const figure = {
        layout: polFigure.layout,
    };

    if (trigger[0].prop_id === "add-location-add.n_clicks" && lastClick) {
        customData = lastClick["points"][0]["customdata"];
        space = customData[3];
        const pointId = generateID();
        data.push({
            uid: pointId,
            x: [meta.minDate, meta.maxDate],
            y: [space, space],
            mode: "lines+markers",
            customdata: [customData],
            name: name,
            line: {
                color: "rgba(171, 32, 253, 0.3)",
                width: 4,
            },
            fixedPoint: true,
            type: "scattergl",
            marker: {
                size: 3,
                cmin: 1,
                cmax: 5,
                color: "#ab20fd",
            },
        });
        let lat = lastClick.points[0]["customdata"][0];
        let lon = lastClick.points[0]["customdata"][1];
        updatedFixedPoints.push({
            lat: [lat],
            lon: [lon],
            marker: {
                cmax: 5,
                cmin: 1,
                color: ["#ab20fd"],
                size: 14,
            },
            name: name,
            mode: "markers",
            type: "scattermapbox",
            uid: pointId,
        });
    }
    else
    if (trigger[0].prop_id === "colocate-store.data" && colocData) 
    {

        for (row of colocData)
        {
            colocs = {
                x: [row[6],row[10]],
                y: [row[13],row[14]],
                name:row[5]+"-"+row[9],
                legendgroup:"colocations",
                mode: 'lines+markers',
                type: "scattergl",
                marker: {
                    color: "rgba(255, 255, 255, 0.1)",
                    size: 15,
                    line:{
                        width:1,
                        color:'#fff'
                    }
                },
                line: {
                    color: "rgba(255, 255, 255, 0.2)",
                    width: 8,
                }
            }
            data.push(colocs)
        }
        

    }
    else
    if (trigger[0].prop_id === "prediction-store.data" && predData) 
    {
        x = []
        y = []
        for (row of predData)
        {
            x.push(row[0])
            y.push(row[15])
        }
        
        preds = {
            x: x,
            y: y,
            name:"predicted",
            legendgroup:"predicted",
            mode: 'markers',
            type: "scattergl",
            marker: {
                color: "rgba(255, 255, 255, 0.1)",
                size: 4,

            }
        }
        data.push(preds)
        
        

    }

    figure.data = data;
    return [figure, updatedFixedPoints];
}