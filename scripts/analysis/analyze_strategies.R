# Load required libraries
library(RSQLite)
library(ggplot2)
library(dplyr)
library(tidyr)

# Set database path
db_path <- "c:/Users/WebkoWuite/DPS/VB Portefeuille Beheer - Documents/Zakelijke Waarden/Aandelen/EU equity/Research/Nederlandse-pensioenfondsen/data/pension_funds.db"

# Connect to database
con <- dbConnect(SQLite(), db_path)

# Query data
query <- "
SELECT f.name, s.region, s.weight_pct 
FROM funds f
JOIN equity_strategies s ON f.id = s.fund_id
"
data <- dbGetQuery(con, query)

# Disconnect
dbDisconnect(con)

# Check if data exists
if (nrow(data) == 0) {
  stop("No strategy data found in database.")
}

# Shorten names for plotting
data$name <- gsub(" \\(.*\\)", "", data$name)
data$name <- gsub("Stichting Pensioenfonds ", "", data$name)

# Create Plot
p <- ggplot(data, aes(x = name, y = weight_pct, fill = region)) +
  geom_bar(stat = "identity", position = "stack") +
  coord_flip() +
  labs(
    title = "Regional Equity Exposure - Top Dutch Pension Funds",
    subtitle = "Based on 2023/2024 Strategy Analysis",
    x = "Pension Fund",
    y = "Weight (%)",
    fill = "Region"
  ) +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    plot.title = element_text(face = "bold", size = 14),
    axis.text.y = element_text(size = 10)
  ) +
  scale_fill_brewer(palette = "Set3")

# Save Plot
output_dir <- "plots"
if (!dir.exists(output_dir)) dir.create(output_dir)
output_path <- file.path(output_dir, "regional_exposure.png")
ggsave(output_path, plot = p, width = 10, height = 6, dpi = 300)

cat(paste("Visualization saved to:", output_path, "\n"))
