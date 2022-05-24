// Simple Node.js script to validate GraphQL schema syntax
const fs = require('fs');
const { buildSchema } = require('graphql');

try {
    const schemaContent = fs.readFileSync('src/main/resources/graphql/schema.graphqls', 'utf8');
    const schema = buildSchema(schemaContent);
    console.log('✓ GraphQL schema is valid!');
    console.log('✓ Alert type found:', schema.getType('Alert') !== undefined);
    console.log('✓ AlertStatus enum found:', schema.getType('AlertStatus') !== undefined);
    
    const alertType = schema.getType('Alert');
    if (alertType) {
        const fields = alertType.getFields();
        console.log('✓ Alert fields:', Object.keys(fields).join(', '));
    }
    
    const alertStatusEnum = schema.getType('AlertStatus');
    if (alertStatusEnum) {
        const values = alertStatusEnum.getValues();
        console.log('✓ AlertStatus values:', values.map(v => v.name).join(', '));
    }
    
    process.exit(0);
} catch (error) {
    console.error('✗ Schema validation failed:', error.message);
    process.exit(1);
}
