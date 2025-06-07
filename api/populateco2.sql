CREATE TABLE co2_factors (
id SERIAL PRIMARY KEY,
source_name TEXT NOT NULL, -- npr. "Electricity - Bosnia", "Natural Gas", "Solar"
country TEXT NOT NULL, -- država na koju se odnosi faktor
co2_factor DECIMAL(10,5) NOT NULL, -- vrijednost faktora u kg CO2 po kWh
unit TEXT DEFAULT 'kgCO2/kWh', -- jedinica mjere
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO co2_factors (source_name, country, co2_factor)
VALUES
('Electricity - Bosnia and Herzegovina', 'Bosnia and Herzegovina', 0.389),
('Electricity - Germany', 'Germany', 0.401),
('Electricity - Sweden', 'Sweden', 0.056),
('Electricity - France', 'France', 0.053),
('Electricity - Poland', 'Poland', 0.715),
('Electricity - USA', 'USA', 0.401),
('Electricity - Norway', 'Norway', 0.019),
('Electricity - Croatia', 'Croatia', 0.131),
('Electricity - Serbia', 'Serbia', 0.643),
('Electricity - Slovenia', 'Slovenia', 0.121);

-- Obriši stare podatke (ako su za države)
DELETE FROM co2_factors;

-- Dodaj nove gradove i njihove co2 faktore
INSERT INTO co2_factors (source_name, country, co2_factor, unit, updated_at)
VALUES
('Electricity - Sarajevo', 'Sarajevo', 0.152, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Mostar', 'Mostar', 0.759, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Banja Luka', 'Banja Luka', 0.577, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Tuzla', 'Tuzla', 0.633, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Zenica', 'Zenica', 0.190, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Bihać', 'Bihać', 0.158, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Brčko', 'Brčko', 0.397, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Livno', 'Livno', 0.334, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Bugojno', 'Bugojno', 0.072, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Trebinje', 'Trebinje', 0.232, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Travnik', 'Travnik', 0.055, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Doboj', 'Doboj', 0.795, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Prijedor', 'Prijedor', 0.412, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Gradiška', 'Gradiška', 0.471, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Cazin', 'Cazin', 0.673, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Goražde', 'Goražde', 0.378, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Bijeljina', 'Bijeljina', 0.110, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Zvornik', 'Zvornik', 0.608, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Čapljina', 'Čapljina', 0.355, 'kgCO2/kWh', CURRENT_TIMESTAMP),
('Electricity - Široki Brijeg', 'Široki Brijeg', 0.549, 'kgCO2/kWh', CURRENT_TIMESTAMP);