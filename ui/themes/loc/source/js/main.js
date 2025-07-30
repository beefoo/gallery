$("#lightgallery")
  .justifiedGallery({
    captions: false,
    rowHeight: 180,
    margins: 8,
  })
  .on("jg.complete", () => {
    lightGallery(
      document.getElementById("lightgallery"),
      {
        plugins: [lgZoom, lgThumbnail],
        speed: 500,
        thumbnail: true,
      }
    );
  });
