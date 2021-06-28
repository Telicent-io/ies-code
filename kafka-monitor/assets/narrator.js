window.dash_clientside = Object.assign({}, window.dash_clientside, {
  clientside: {
    isCreateDisabled: function (id, time, lat, lon) {
      if (id && time && lat && lon) {
        return false;
      }
      return true;
    },
    toggleModal: function (open, close, create, isOpen) {
      if (open || close || create) {
        return !isOpen;
      }
      return isOpen;
    },
    updateClicked: function (clickData, meta) {
      let lat = meta.centLat;
      let lon = meta.centLon;
      let inputText = "";
      const lastClick = clickData;
      if (clickData) {
        lat = clickData["points"][0]["customdata"][0];
        lon = clickData["points"][0]["customdata"][1];
        inputText = `${clickData["points"][0]["customdata"][2]} - ${clickData["points"][0]["customdata"][4]}`;
      }
      lat = lat.toFixed(6);
      lon = lon.toFixed(6);
      return [lat, lon, inputText, lastClick];
    },
    onAddPoint: function (addPloiNClicks,colocData,predData,polFigure,name, meta, lastClick, fixedPoints)
    {
      return addPoint(addPloiNClicks,colocData,predData,polFigure,name,meta,lastClick,fixedPoints)
      
    },

    updateMap: function (
      hd2d,
      cd2d,
      style,
      fixedPoints,
      showDensity,
      oldMap,
      meta,
      graph
    ) {
      let trigger = dash_clientside.callback_context.triggered;

      if (style !== oldMap.layout.mapbox.style) {
        oldMap.layout.mapbox.style = style;
        oldMap.layout.template.layout.mapbox.style = style;
      }
      if (trigger.length === 0) {
        return oldMap;
      } else if (
        trigger[0].prop_id === "fixed-point-store.data" &&
        trigger[0].value.length === 0
      ) {
        return oldMap;
      }

      if (trigger[0].prop_id === "fixed-point-store.data") {
        const newFixedPos = trigger[0].value.find((item) =>
          oldMap.data.every((marker) => marker.uid !== item.uid)
        );
        const newData = oldMap.data;
        newData.push(newFixedPos);
        const newMap = {
          data: newData,
          layout: oldMap.layout,
        };
        return newMap;
      }
      let data = oldMap.data.filter((node) => node.uid != "hoveredPoint");

      let newMap = {
        layout: oldMap.layout,
      };
      if (
        trigger[0].prop_id === "PoL.hoverData" ||
        trigger[0].prop_id === "PoL.clickData"
      ) {
        let lat = hd2d.points[0]["customdata"][0];
        let lon = hd2d.points[0]["customdata"][1];
        let id = hd2d.points[0]["customdata"][2];
        let color = meta["colourDict"][id]["bright"];

        data.push({
          lat: [lat],
          lon: [lon],
          marker: {
            cmax: 5,
            cmin: 1,
            color: [color],
            size: 14,
          },
          name: id,
          mode: "markers",
          type: "scattermapbox",
          uid: "hoveredPoint",
        });
      }

      if (trigger[0].prop_id === "PoL.clickData") {
        let lat = cd2d.points[0]["customdata"][0];
        let lon = cd2d.points[0]["customdata"][1];
        newMap.layout.mapbox["center"] = {
          lat,
          lon,
        };
      }

      if (
        Array.isArray(showDensity) &&
        trigger[0].prop_id === "showDensity.value"
      ) {
        data = data.map((layer) => {
          if (layer.type === "densitymapbox") {
            layer.visible = showDensity.includes("show");
          }
          return layer;
        });
      }

      newMap.data = data;

      return newMap;
    },
  },
});
function generateID() {
  return "_" + Math.random().toString(36).substr(2, 9);
}
