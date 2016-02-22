
function DragSave(elemID,successFunc,HoverStyle) {
if(window.FileReader) { 
			var drop; 
			addEventHandler(window, 'load', function() {
				//var status = document.getElementById('status');
				drop   = document.getElementById(elemID);
				
				
					var oldStyle = drop.style.border ;
					function cancelColor(e) {
            if (typeof HoverStyle != 'undefined') {
              drop.style.border = HoverStyle;
            }
						if (e.preventDefault) { e.preventDefault(); }
						return false;
					}
					function cancelUnColor(e) {
            if (typeof HoverStyle != 'undefined') {
              drop.style.border = oldStyle
            }
						if (e.preventDefault) { e.preventDefault(); }
						return false;
					}
          	// Tells the browser that we *can* drop on this target
				
          addEventHandler(drop, 'dragover', cancelColor);
          addEventHandler(drop, 'dragenter', cancelColor);
          addEventHandler(drop, 'dragexit', cancelUnColor);
          addEventHandler(drop, 'dragleave', cancelUnColor);

			

				addEventHandler(drop, 'drop', function (e) {
					
					e = e || window.event; // get window.event if e argument missing (in IE)   
					if (e.preventDefault) { e.preventDefault(); } // stops the browser from redirecting off to the image.
					drop.style.border = oldStyle
					var dt    = e.dataTransfer;
					var files = dt.files;
					for (var i=0; i<files.length; i++) {
						var file = files[i];
						var reader = new FileReader();

						//attach event handlers here...
						//drop.innerHTML = file.name; 
						reader.readAsDataURL(file);
						addEventHandler(reader, 'loadend', function(e, file) {
							successFunc(file,files.length)
							
						}.bindToEventHandler(file));
					}
					return false;
				});
				//What does this do again??
				Function.prototype.bindToEventHandler = function bindToEventHandler() {
					var handler = this;
					var boundParameters = Array.prototype.slice.call(arguments);
					//create closure
					return function(e) {
						e = e || window.event; // get window.event if e argument missing (in IE)   
						boundParameters.unshift(e);
						handler.apply(this, boundParameters);
					}
				};
			});
		
		
		} else { 
		  document.getElementById('drop').innerHTML = '<form class="FileSubmit" style="margin : 1px;" action="/EmailListUpload" method="post" enctype="multipart/form-data">'+
				'<p>Upload a new recipient file</p>'+
				'<input style="margin : 1px;" type="file" name="File"><br>'+
				'<input style="margin : 1px;" type="submit" onclick="requestedToMailList = true;" value="Submit">'+
				'</form>';
		}
}
function DragUpload(elemID,url,successFunc) {
	DragSave(elemID,function(file) {
		var formData = new FormData({EmailFile:file});
		formData.append('File',file,file.name);
		$.ajax({
			url: url,
			type: 'POST',
			data: formData,
			async: false,
			cache: false,
			contentType: false,
			processData: false,
			success: successFunc,
			error: function(){
				alert("error in ajax form submission");
			}
		});
	})
}


function addEventHandler(obj, evt, handler) {
			if(obj.addEventListener) {
				// W3C method
				obj.addEventListener(evt, handler, false);
			} else if(obj.attachEvent) {
				// IE method.
				obj.attachEvent('on'+evt, handler);
			} else {
				// Old school method.
				obj['on'+evt] = handler;
			}
		}