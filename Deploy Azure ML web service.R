library(AzureML)
install.packages('devtools')
library(devtools)

# Create glm model with `mtcars` dataset
carsModel <- glm(formula = am ~ hp + wt, data = mtcars, family = binomial)

# Produce a prediction function that can use the model
manualTransmission <- function(newdata) {
  predict(carsModel, newdata, type = "response")
}

# test function locally by printing results
print(manualTransmission(mtcars))

# Get these from Azure ML workspace #

wsID = 'c293dbad25c4488cbcb03402b227b4c8'
wsAuth = 'raZ1C/tlqVjosNN2mflC+oXnjVRDD1XVcAymr2MEVOx0Bzcbzwkw5KxfW8TkCqwCiWdN7U8nnDKJQWgM+HwiUw=='

wsObj = workspace(wsID, wsAuth)

# create web service 

mtcars_schema <- mtcars[,c(4,6)]

carswebservice1 <- publishWebService(
  wsObj,
  fun = manualTransmission,
  name = 'carswebservice1',
  inputSchema = mtcars_schema
)

# Get web authorization key to authorize the application to query the web service #
head(carswebservice1)
