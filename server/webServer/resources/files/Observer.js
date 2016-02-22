	function jq( myid ) {
			return "#" + myid.replace( /(:|\.|\[|\]|,|\/|\\)/g, "\\$1" );
		}	
			
		function tco(f) {
				var value;
				var active = false;
				var accumulated = [];

				return function accumulator() {
					accumulated.push(arguments);

					if (!active) {
						active = true;

						while (accumulated.length) {
							value = f.apply(this, accumulated.shift());
						}

						active = false;

						return value;
					}
				}
			}			
	
	var id = -100000	

function Observe(path,doThis) {	

	$(document).ready(function(){
		RegisterObserver()
	});

	function RegisterObserver() {
		$.ajax({
				url: '/Observe/New/'+path,
				type: 'GET',
				async: true,
				cache: false,
				contentType: false,
				processData: false,
				success: function(retId){
					id = JSON.parse(retId)
					DoUpdateObserver() 
				},
				error: function(err){
					alert("failed to RegisterObserver");
				}
			});
	
	};
	
	
	DoUpdateObserver = tco(function () {
			$.ajax({
				url: '/Observe/Get/'+path,
				type: 'GET',
				async: true,
				cache: false,
				data: {
					id : id
				},
				contentType: true,
				processData: true,
				success: function(body){
					list =  JSON.parse(body)
					doThis(list);
					DoUpdateObserver()
				},
				error: function(err){
					
				}
			})
	});
}