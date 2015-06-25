@echo !!Output Starting Import
@echo !!Output Checking for HEC installation...
@set install_location=%ProgramFiles%
@if DEFINED ProgramFiles(x86) set install_location=%Programfiles(x86)%

@IF NOT EXIST "%install_location%\HEC\HEC-DSSVue" (

 @echo "<plugin_result><message>Error</message><plugin_name>ImportDSS</plugin_name><network_id></network_id><errors><error>Cannot find HEC DSSVue at %install_location% Please install it.</error></errors><warnings></warnings><files></files></plugin_result>"
 
 @exit /B
 )

@echo !!Output HEC found. Running import code...

@set hec_java=%install_location%\HEC\HEC-DSSVue\java\bin\java
@set path=%install_location%\HEC\HEC-DSSVue\lib
@set this_path=%~dp0

@echo %this_path%

CALL "%hec_java%" -Djava.library.path="%path%" -jar "%this_path%DSSExport.jar" %*
@echo !!Output complete
